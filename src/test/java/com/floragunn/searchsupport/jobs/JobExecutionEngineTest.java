package com.floragunn.searchsupport.jobs;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;

import com.floragunn.searchguard.test.SingleClusterTest;
import com.floragunn.searchsupport.jobs.config.DefaultJobConfig;
import com.floragunn.searchsupport.jobs.config.DefaultJobConfigFactory;

public class JobExecutionEngineTest extends SingleClusterTest {

    @Test
    public void simpleTest() throws Exception {
        final Settings settings = Settings.builder().build();

        setup(settings);

        try (TransportClient tc = getInternalTransportClient()) {
            // tc.admin().indices().create(new CreateIndexRequest("testjobconfig")).actionGet();         
            tc.index(new IndexRequest("testjobconfig").setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                    .source("{\"trigger\": {\"schedule\": {\"cron\": \"0/2 * * * * ?\"}}}", XContentType.JSON)).actionGet();

            Scheduler scheduler = new SchedulerBuilder<DefaultJobConfig>().client(tc).name("test").configIndex("testjobconfig")
                    .jobFactory(new DefaultJobConfigFactory(TestJob.class)).build();

            scheduler.start();

            Thread.sleep(2000000);
        }

    }

    public static class TestJob implements Job {

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            System.out.println("job: " + context + " " + context.getMergedJobDataMap());
        }

    }

}
