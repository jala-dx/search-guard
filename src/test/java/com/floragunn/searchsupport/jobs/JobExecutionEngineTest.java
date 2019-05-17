package com.floragunn.searchsupport.jobs;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.node.PluginAwareNode;
import org.junit.Test;
import org.mockito.Mockito;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchsupport.jobs.config.DefaultJobConfig;
import com.floragunn.searchsupport.jobs.config.DefaultJobConfigFactory;

public class JobExecutionEngineTest extends SingleClusterTest {

    @Test
    public void simpleTest() throws Exception {
        final Settings settings = Settings.builder().build();

        setup(settings);

        try (TransportClient tc = getInternalTransportClient()) {

            tc.index(new IndexRequest("testjobconfig").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"trigger\": {\"schedule\": {\"cron\": \"0/1 * * * * ?\"}}}", XContentType.JSON)).actionGet();

            for (PluginAwareNode node : this.clusterHelper.allNodes()) {
                ClusterService clusterService = node.injector().getInstance(ClusterService.class);

                Scheduler scheduler = new SchedulerBuilder<DefaultJobConfig>().client(tc).name("test_" + clusterService.getNodeName())
                        .configIndex("testjobconfig").jobFactory(new DefaultJobConfigFactory(TestJob.class)).distributed(clusterService).build();

                scheduler.start();
            }

            Thread.sleep(15 * 1000);

            this.clusterHelper.allNodes().get(1).close();

            Thread.sleep(15 * 1000);

        }

    }

    public static class TestJob implements Job {

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            try {
                System.out.println("job: " + context + " " + context.getMergedJobDataMap() + " on " + context.getScheduler().getSchedulerName());
            } catch (SchedulerException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

}
