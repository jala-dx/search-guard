package com.floragunn.searchsupport.jobs;

import java.util.Map;
import java.util.Set;

public interface JobConfigListener<JobType extends com.floragunn.searchsupport.jobs.config.JobConfig> {

    void onChange(Set<JobType> newJobs, Map<JobType, JobType> updatedJobs, Set<JobType> deletedJobs);
}
