package com.floragunn.searchsupport.jobs;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.JobStore;
import org.quartz.spi.SchedulerPlugin;
import org.quartz.spi.ThreadPool;

import com.floragunn.searchsupport.jobs.config.IndexJobConfigSource;
import com.floragunn.searchsupport.jobs.config.JobConfig;
import com.floragunn.searchsupport.jobs.config.JobConfigFactory;
import com.floragunn.searchsupport.jobs.core.IndexJobStateStore;

public class SchedulerBuilder<JobType extends JobConfig> {

    private String name;
    private String configIndex;
    private String stateIndex;
    private Client client;
    private int maxThreads = 3;
    private int threadPriority = Thread.NORM_PRIORITY;
    private JobConfigFactory<JobType> jobFactory;
    private Iterable<JobType> jobConfigSource;
    private JobStore jobStore;
    private ThreadPool threadPool;
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

        if (this.jobConfigSource == null) {
            this.jobConfigSource = new IndexJobConfigSource<>(configIndex, client, jobFactory);
        }

        if (this.jobStore == null) {
            this.jobStore = new IndexJobStateStore<>(stateIndex, client, jobConfigSource, jobFactory);
        }

        if (this.threadPool == null) {
            this.threadPool = new SimpleThreadPool(this.maxThreads, threadPriority);
        }

        DirectSchedulerFactory.getInstance().createScheduler(this.name, this.name, threadPool, jobStore, schedulerPluginMap, null, -1, -1, -1, false,
                null);

        // TODO well, change this somehow:

        return DirectSchedulerFactory.getInstance().getScheduler(this.name);
    }
}