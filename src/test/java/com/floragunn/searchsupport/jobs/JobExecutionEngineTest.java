package com.floragunn.searchsupport.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.node.PluginAwareNode;
import org.junit.Test;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchsupport.jobs.cluster.NodeNameComparator;
import com.floragunn.searchsupport.jobs.config.DefaultJobConfig;

public class JobExecutionEngineTest extends SingleClusterTest {
    private static final Logger log = LogManager.getLogger(JobExecutionEngineTest.class);

    @Test
    public void test() throws Exception {
        final Settings settings = Settings.builder().build();

        setup(settings);

        try (Client tc = getInternalTransportClient()) {

            tc.index(new IndexRequest("testjobconfig").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"hash\": 1, \"trigger\": {\"schedule\": {\"cron\": \"*/2 * * * * ?\"}}}", XContentType.JSON)).actionGet();

            for (PluginAwareNode node : this.clusterHelper.allNodes()) {
                ClusterService clusterService = node.injector().getInstance(ClusterService.class);

                Scheduler scheduler = new SchedulerBuilder<DefaultJobConfig>().client(tc).name("test_" + clusterService.getNodeName())
                        .configIndex("testjobconfig").jobFactory(new ConstantHashJobConfig.Factory(LoggingTestJob.class)).distributed(clusterService)
                        .nodeComparator(new NodeNameComparator(clusterService)).build();

                scheduler.start();
            }

            Thread.sleep(15 * 1000);

            this.clusterHelper.allNodes().get(1).close();

            Thread.sleep(15 * 1000);

            clusterHelper.stopCluster();

        }

    }

    @Test
    public void basicTest() throws Exception {
        final Settings settings = Settings.builder().build();

        setup(settings);

        try (Client tc = getInternalTransportClient()) {

            String jobConfig = createJobConfig(1, "basic", null, "*/1 * * * * ?");

            tc.index(new IndexRequest("testjobconfig").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(jobConfig, XContentType.JSON)).actionGet();

            for (PluginAwareNode node : this.clusterHelper.allNodes()) {
                ClusterService clusterService = node.injector().getInstance(ClusterService.class);

                Scheduler scheduler = new SchedulerBuilder<DefaultJobConfig>().client(tc).name("test_" + clusterService.getNodeName())
                        .configIndex("testjobconfig").jobFactory(new ConstantHashJobConfig.Factory(TestJob.class)).distributed(clusterService)
                        .nodeComparator(new NodeNameComparator(clusterService)).build();

                scheduler.start();
            }

            Thread.sleep(5 * 1000);

            int count = TestJob.getCounter("basic");

            assertTrue("count is " + count, count >= 4 && count <= 8);

            clusterHelper.stopCluster();

        }

    }

    @Test
    public void nonConcurrentTest() throws Exception {
        final Settings settings = Settings.builder().build();

        setup(settings);

        try (Client tc = getInternalTransportClient()) {

            String jobConfig = createJobConfig(1, "nonconcurrent", 1500, "*/1 * * * * ?");

            tc.index(new IndexRequest("testjobconfig").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(jobConfig, XContentType.JSON)).actionGet();

            for (PluginAwareNode node : this.clusterHelper.allNodes()) {
                ClusterService clusterService = node.injector().getInstance(ClusterService.class);

                Scheduler scheduler = new SchedulerBuilder<DefaultJobConfig>().client(tc).name("test_" + clusterService.getNodeName())
                        .configIndex("testjobconfig").jobFactory(new ConstantHashJobConfig.Factory(NonConcurrentTestJob.class))
                        .distributed(clusterService).nodeComparator(new NodeNameComparator(clusterService)).build();

                scheduler.start();
            }

            Thread.sleep(5 * 1000);

            assertEquals(1, TestJob.getCounter("nonconcurrent_max_concurrency"));

            int count = TestJob.getCounter("nonconcurrent");

            assertTrue("count is " + count, count >= 4 && count <= 8);

            clusterHelper.stopCluster();

        }

    }

    @Test
    public void nodeFilterTest() throws Exception {
        final Settings settings = Settings.builder().build();

        setup(settings);

        try (Client tc = getInternalTransportClient()) {

            String jobConfig = createJobConfig(1, "nodeFilterTest", null, "*/1 * * * * ?");

            tc.index(new IndexRequest("testjobconfig").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(jobConfig, XContentType.JSON)).actionGet();

            for (PluginAwareNode node : this.clusterHelper.allNodes()) {
                ClusterService clusterService = node.injector().getInstance(ClusterService.class);

                Scheduler scheduler = new SchedulerBuilder<DefaultJobConfig>().client(tc).name("test_" + clusterService.getNodeName())
                        .nodeFilter("node_group_1:a").configIndex("testjobconfig").jobFactory(new ConstantHashJobConfig.Factory(TestJob.class))
                        .distributed(clusterService).nodeComparator(new NodeNameComparator(clusterService)).build();

                scheduler.start();

            }

            Thread.sleep(5 * 1000);

            int count = TestJob.getCounter("nodeFilterTest");

            assertTrue("count is " + count, count >= 4 && count <= 8);

            clusterHelper.stopCluster();

        }

    }

    @Test
    public void emptyNodeFilterTest() throws Exception {
        final Settings settings = Settings.builder().build();

        setup(settings);

        try (Client tc = getInternalTransportClient()) {

            String jobConfig = createJobConfig(1, "emptyNodeFilterTest", null, "*/1 * * * * ?");

            tc.index(new IndexRequest("testjobconfig").setRefreshPolicy(RefreshPolicy.IMMEDIATE).source(jobConfig, XContentType.JSON)).actionGet();

            for (PluginAwareNode node : this.clusterHelper.allNodes()) {
                ClusterService clusterService = node.injector().getInstance(ClusterService.class);

                Scheduler scheduler = new SchedulerBuilder<DefaultJobConfig>().client(tc).name("test_" + clusterService.getNodeName())
                        .nodeFilter("node_group_1:xxx").configIndex("testjobconfig").jobFactory(new ConstantHashJobConfig.Factory(TestJob.class))
                        .distributed(clusterService).nodeComparator(new NodeNameComparator(clusterService)).build();

                scheduler.start();

            }

            Thread.sleep(5 * 1000);

            int count = TestJob.getCounter("emptyNodeFilterTest");

            assertEquals(0, count);

            clusterHelper.stopCluster();

        }

    }

    private String createJobConfig(int hash, String name, Integer delay, String... cronSchedule) {
        StringBuilder result = new StringBuilder("{");

        result.append("\"hash\": ").append(hash).append(",");

        if (name != null) {
            result.append("\"name\": \"").append(name).append("\",");
        }

        if (delay != null) {
            result.append("\"delay\": ").append(delay).append(",");
        }

        result.append("\"trigger\": {\"schedule\": {\"cron\": ");

        if (cronSchedule.length == 1) {
            result.append("\"").append(cronSchedule[0]).append("\"");
        } else {
            result.append("[");
            boolean first = true;
            for (String cron : cronSchedule) {
                if (first) {
                    first = false;
                } else {
                    result.append(",");
                }
                result.append("\"").append(cron).append("\"");

            }
            result.append("]");
        }

        result.append("}}}");

        return result.toString();
    }

    public static class LoggingTestJob implements Job {

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            try {
                System.out.println(
                        "job: " + context + " " + new HashMap<>(context.getMergedJobDataMap()) + " on " + context.getScheduler().getSchedulerName());
                System.out.println(context.getJobDetail());
            } catch (SchedulerException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    @Override
    protected Settings.Builder minimumSearchGuardSettingsBuilder(int node, boolean sslOnly) {
        Settings.Builder builder = super.minimumSearchGuardSettingsBuilder(node, sslOnly);

        builder.put("node.attr.node_index", node);

        if (node == 1 || node == 2) {
            builder.put("node.attr.node_group_1", "a");
        } else {
            builder.put("node.attr.node_group_1", "b");
        }

        if (node == 2 || node == 3) {
            builder.put("node.attr.node_group_2", "x");
        } else {
            builder.put("node.attr.node_group_2", "y");
        }

        return builder;
    }

    public static class TestJob implements Job {

        static Map<String, Integer> counters = new ConcurrentHashMap<>();

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            String name = context.getMergedJobDataMap().getString("name");

            if (name != null) {
                incrementCounter(name);
                int maxConcurrency = incrementCounter(name + "_active_concurrent");

                if (maxConcurrency > getCounter(name + "_max_concurrency")) {
                    setCounter(name + "_max_concurrency", maxConcurrency);
                }

                log.info("JOB " + name + " #" + getCounter(name));
            }

            Number delay = (Number) context.getMergedJobDataMap().get("delay");

            if (delay != null) {
                try {
                    Thread.sleep(delay.longValue());
                } catch (InterruptedException e) {

                }
            }

            decrementCounter(name + "_active_concurrent");
        }

        static int incrementCounter(String counterName) {
            int value = getCounter(counterName) + 1;
            counters.put(counterName, value);
            return value;
        }

        static int decrementCounter(String counterName) {
            int value = getCounter(counterName) - 1;
            counters.put(counterName, value);
            return value;
        }

        static void setCounter(String counterName, int number) {
            counters.put(counterName, number);
        }

        static int getCounter(String counterName) {
            Integer value = counters.get(counterName);

            if (value == null) {
                return 0;
            } else {
                return value;
            }
        }
    }

    @DisallowConcurrentExecution
    public static class NonConcurrentTestJob extends TestJob {
        int prevMaxConcurrency = 0;

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            String name = context.getMergedJobDataMap().getString("name");

            super.execute(context);

            if (name != null) {
                int maxConcurrency = getCounter(name + "_max_concurrency");

                if (maxConcurrency > prevMaxConcurrency && maxConcurrency > 1) {
                    log.error("DisallowConcurrentExecution constraint violated during last job run of " + name + " " + maxConcurrency + " ("
                            + prevMaxConcurrency + ")");
                    prevMaxConcurrency = maxConcurrency;
                }

            }

            log.info("JOB " + name + " finished");
        }

    }

}
