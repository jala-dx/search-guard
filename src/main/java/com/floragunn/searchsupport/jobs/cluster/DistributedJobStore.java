package com.floragunn.searchsupport.jobs.cluster;

import org.quartz.spi.JobStore;

public interface DistributedJobStore extends JobStore {
    void clusterConfigChanged();
}
