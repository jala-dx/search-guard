package com.floragunn.searchsupport.jobs;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.node.PluginAwareNode;
import org.junit.Test;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchsupport.jobs.cluster.NodeNameComparator;
import com.floragunn.searchsupport.jobs.config.DefaultJobConfig;

public class JobExecutionEngineTest extends SingleClusterTest {

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

            tc.index(new IndexRequest("testjobconfig").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"hash\": 1, \"counter\": \"basic\", \"trigger\": {\"schedule\": {\"cron\": \"*/2 * * * * ?\"}}}", XContentType.JSON))
                    .actionGet();

            for (PluginAwareNode node : this.clusterHelper.allNodes()) {
                ClusterService clusterService = node.injector().getInstance(ClusterService.class);

                Scheduler scheduler = new SchedulerBuilder<DefaultJobConfig>().client(tc).name("test_" + clusterService.getNodeName())
                        .configIndex("testjobconfig").jobFactory(new ConstantHashJobConfig.Factory(TestJob.class)).distributed(clusterService)
                        .nodeComparator(new NodeNameComparator(clusterService)).build();

                scheduler.start();
            }

            Thread.sleep(5 * 1000);

            clusterHelper.stopCluster();

            int count = TestJob.getCounter("basic");

            assertTrue(count >= 2 && count <= 5);

        }

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

    public static class TestJob implements Job {

        static Map<String, Integer> counters = new ConcurrentHashMap<>();

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            String counter = context.getMergedJobDataMap().getString("counter");

            if (counter != null) {
                incrementCounter(counter);
            }
        }

        static void incrementCounter(String counterName) {
            counters.put(counterName, getCounter(counterName) + 1);
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

}
