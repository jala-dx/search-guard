package com.floragunn.searchsupport.jobs;

import org.quartz.Job;

import com.floragunn.searchsupport.jobs.config.DefaultJobConfig;
import com.floragunn.searchsupport.jobs.config.DefaultJobConfigFactory;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.jayway.jsonpath.ReadContext;

public class ConstantHashJobConfig extends DefaultJobConfig {

    private int hashCode;

    public ConstantHashJobConfig(Class<? extends Job> jobClass) {
        super(jobClass);
    }

    public int hashCode() {
        return hashCode;
    }

    public static class Factory extends DefaultJobConfigFactory {

        public Factory(Class<? extends Job> jobClass) {
            super(jobClass);
        }

        @Override
        protected DefaultJobConfig createObject(String id, ReadContext ctx) {
            return new ConstantHashJobConfig(getJobClass(ctx));
        }

        @Override
        protected DefaultJobConfig createFromReadContext(String id, ReadContext ctx, long version) throws ConfigValidationException {
            ConstantHashJobConfig result = (ConstantHashJobConfig) super.createFromReadContext(id, ctx, version);

            Integer hashCode = ctx.read("$.hash", Integer.class);

            if (hashCode != null) {
                result.hashCode = hashCode;
            } else {
                result.hashCode = result.getJobKey().hashCode();
            }

            return result;
        }
    }
}
