package com.floragunn.searchsupport.jobs.config;

import org.quartz.Job;
import org.quartz.JobKey;

import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.jayway.jsonpath.ReadContext;

public class DefaultJobConfigFactory extends AbstractJobConfigFactory<DefaultJobConfig> implements JobConfigFactory<DefaultJobConfig> {

    public DefaultJobConfigFactory(Class<? extends Job> jobClass) {
        super(jobClass);
    }

    protected DefaultJobConfig createObject(String id, ReadContext ctx) {
        return new DefaultJobConfig(getJobClass(ctx));
    }

    protected DefaultJobConfig createFromReadContext(String id, ReadContext ctx, long version) throws ConfigValidationException {
        DefaultJobConfig result = createObject(id, ctx);
        JobKey jobKey = getJobKey(id, ctx);

        result.setJobKey(jobKey);

        result.setDescription(getDescription(ctx));

        Boolean durability = getDurability(ctx);

        if (durability != null) {
            result.setDurable(durability);
        }

        result.setJobDataMap(getJobDataMap(ctx));
        result.setTriggers(getTriggers(jobKey, ctx));
        result.setVersion(version);
        result.setAuthToken(getAuthToken(ctx));

        return result;
    }
}
