package com.floragunn.searchsupport.jobs;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.SchedulerPlugin;
import org.quartz.spi.ThreadPool;

import com.floragunn.searchsupport.jobs.cluster.DistributedJobStore;
import com.floragunn.searchsupport.jobs.cluster.JobDistributor;
import com.floragunn.searchsupport.jobs.config.IndexJobConfigSource;
import com.floragunn.searchsupport.jobs.config.JobConfig;
import com.floragunn.searchsupport.jobs.config.JobConfigFactory;
import com.floragunn.searchsupport.jobs.core.IndexJobStateStore;

public class SchedulerBuilder<JobType extends JobConfig> {
    private final static Logger log = LogManager.getLogger(SchedulerBuilder.class);

    private String name;
    private String configIndex;
    private String stateIndex;
    private Client client;
    private int maxThreads = 3;
    private int threadPriority = Thread.NORM_PRIORITY;
    private JobConfigFactory<JobType> jobFactory;
    private Iterable<JobType> jobConfigSource;
    private JobStore jobStore;
    private JobDistributor jobDistributor;
    private ThreadPool threadPool;
    private String nodeFilter;
    private ClusterService clusterService;
    private Map<String, SchedulerPlugin> schedulerPluginMap = new HashMap<>();

    public SchedulerBuilder<JobType> name(String name) {
        this.name = name;
        return this;
    }

    public SchedulerBuilder<JobType> configIndex(String configIndex) {
        this.configIndex = configIndex;
        return this;
    }

    public SchedulerBuilder<JobType> stateIndex(String stateIndex) {
        this.stateIndex = stateIndex;
        return this;
    }

    public SchedulerBuilder<JobType> distributed(ClusterService clusterService) {
        this.clusterService = clusterService;
        return this;
    }

    public SchedulerBuilder<JobType> nodeFilter(String nodeFilter) {
        this.nodeFilter = nodeFilter;
        return this;
    }

    public SchedulerBuilder<JobType> client(Client client) {
        this.client = client;
        return this;
    }

    public SchedulerBuilder<JobType> maxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
        return this;
    }

    public SchedulerBuilder<JobType> threadPriority(int threadPriority) {
        this.threadPriority = threadPriority;
        return this;
    }

    public SchedulerBuilder<JobType> jobFactory(JobConfigFactory<JobType> jobFactory) {
        this.jobFactory = jobFactory;
        return this;
    }

    public SchedulerBuilder<JobType> jobConfigSource(Iterable<JobType> jobConfigSource) {
        this.jobConfigSource = jobConfigSource;
        return this;
    }

    public SchedulerBuilder<JobType> jobStore(JobStore jobStore) {
        this.jobStore = jobStore;
        return this;
    }

    public SchedulerBuilder<JobType> threadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
        return this;
    }

    public Scheduler build() throws SchedulerException {
        if (this.configIndex == null) {
            this.configIndex = name;
        }

        if (this.stateIndex == null) {
            this.stateIndex = this.configIndex + "_state";
        }

        if (this.jobDistributor == null && clusterService != null) {
            this.jobDistributor = new JobDistributor(name, nodeFilter, clusterService, null);
        }

        if (this.jobConfigSource == null) {
            this.jobConfigSource = new IndexJobConfigSource<>(configIndex, client, jobFactory, jobDistributor);
        }

        if (this.jobStore == null) {
            this.jobStore = new IndexJobStateStore<>(stateIndex, client, jobConfigSource, jobFactory);
        }

        if (this.jobStore instanceof DistributedJobStore && this.jobDistributor != null) {
            this.jobDistributor.setDistributedJobStore((DistributedJobStore) this.jobStore);
        }

        if (this.threadPool == null) {
            this.threadPool = new SimpleThreadPool(maxThreads, threadPriority);
        }

        schedulerPluginMap.put(CleanupSchedulerPlugin.class.getName(), new CleanupSchedulerPlugin(clusterService, jobDistributor));

        DirectSchedulerFactory.getInstance().createScheduler(name, name, threadPool, jobStore, schedulerPluginMap, null, -1, -1, -1, false, null);

        // TODO well, change this somehow:

        return DirectSchedulerFactory.getInstance().getScheduler(name);
    }

    private static class CleanupSchedulerPlugin implements SchedulerPlugin {

        private Scheduler scheduler;
        private JobDistributor jobDistributor;
        private ClusterService clusterService;

        CleanupSchedulerPlugin(ClusterService clusterService, JobDistributor jobDistributor) {
            this.jobDistributor = jobDistributor;
            this.clusterService = clusterService;
        }

        @Override
        public void initialize(String name, Scheduler scheduler, ClassLoadHelper loadHelper) throws SchedulerException {
            this.scheduler = scheduler;

            if (this.clusterService != null) {

                this.clusterService.addLifecycleListener(new LifecycleListener() {
                    public void beforeStop() {
                        log.info("Shutting down scheduler " + CleanupSchedulerPlugin.this.scheduler + " because node is going down");

                        try {
                            // TODO wait for jobs to complete?
                            scheduler.shutdown();
                        } catch (Exception e) {
                            log.error("Error while shutting down scheduler " + CleanupSchedulerPlugin.this.scheduler, e);
                        }
                    }
                });
            }
        }

        @Override
        public void start() {

        }

        @Override
        public void shutdown() {
            if (this.jobDistributor != null) {
                try {
                    this.jobDistributor.close();
                    this.jobDistributor = null;
                } catch (Exception e) {
                    log.warn("Error while closing jobDistributor", e);
                }
            }
        }
    }

}