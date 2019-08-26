package com.floragunn.searchsupport.jobs;

import java.text.ParseException;

import org.quartz.Job;

import com.floragunn.searchsupport.jobs.config.DefaultJobConfig;
import com.floragunn.searchsupport.jobs.config.DefaultJobConfigFactory;
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
        protected DefaultJobConfig createObject() {
            return new ConstantHashJobConfig(getJobClass());
        }

        @Override
        protected DefaultJobConfig createFromReadContext(String id, ReadContext ctx) throws ParseException {
            ConstantHashJobConfig result = (ConstantHashJobConfig) super.createFromReadContext(id, ctx);

            Integer hashCode = ctx.read("hash", Integer.class);

            if (hashCode != null) {
                result.hashCode = hashCode;
            } else {
                result.hashCode = result.getJobKey().hashCode();
            }

            return result;
        }
    }
}
