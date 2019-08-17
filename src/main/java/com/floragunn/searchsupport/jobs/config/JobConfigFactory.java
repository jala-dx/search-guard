package com.floragunn.searchsupport.jobs.config;

import java.io.IOException;
import java.text.ParseException;

import org.elasticsearch.common.bytes.BytesReference;
import org.quartz.JobDetail;

public interface JobConfigFactory<JobConfigType extends JobConfig> {
    JobConfigType createFromBytes(String id, BytesReference source, long version) throws ParseException, IOException;

    JobDetail createJobDetail(JobConfigType jobType);
}
