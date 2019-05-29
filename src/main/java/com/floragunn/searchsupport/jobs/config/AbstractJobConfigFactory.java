package com.floragunn.searchsupport.jobs.config;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.elasticsearch.common.bytes.BytesReference;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.TypeRef;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;

public abstract class AbstractJobConfigFactory<JobConfigType extends JobConfig> implements JobConfigFactory<JobConfigType> {
    protected String group = "main";
    protected Class<? extends Job> jobClass;

    // TODO parse paths
    protected String descriptionPath = "$.description";
    protected String durablePath = "$.durable";
    protected String cronScheduleTriggerPath = "$.trigger.schedule.cron";
    protected String jobDataPath = "$";
    protected final static Configuration JSON_PATH_CONFIG = Configuration.builder().options(com.jayway.jsonpath.Option.SUPPRESS_EXCEPTIONS)
            .jsonProvider(new JacksonJsonNodeJsonProvider()).mappingProvider(new JacksonMappingProvider()).build();
    protected final static TypeRef<Map<String, Object>> MAP_TYPE_REF = new TypeRef<Map<String, Object>>() {
    };

    public AbstractJobConfigFactory(Class<? extends Job> jobClass) {
        this.jobClass = jobClass;
    }

    @Override
    public JobConfigType createFromBytes(String id, BytesReference source) throws ParseException {
        ReadContext ctx = JsonPath.using(JSON_PATH_CONFIG).parse(source.utf8ToString());

        return createFromReadContext(id, ctx);
    }

    @Override
    public JobDetail createJobDetail(JobConfigType jobType) {
        JobBuilder jobBuilder = JobBuilder.newJob(jobType.getJobClass());

        jobBuilder.withIdentity(jobType.getJobKey());

        if (jobType.getJobDataMap() != null) {
            jobBuilder.setJobData(new JobDataMap(jobType.getJobDataMap()));
        }

        jobBuilder.withDescription(jobType.getDescription());
        jobBuilder.storeDurably(jobType.isDurable());
        return jobBuilder.build();
    }

    abstract protected JobConfigType createFromReadContext(String id, ReadContext ctx) throws ParseException;

    protected JobKey getJobKey(String id, ReadContext ctx) {
        return new JobKey(id, group);
    }

    protected String getDescription(ReadContext ctx) {
        if (this.descriptionPath != null) {
            return ctx.read(descriptionPath, String.class);
        } else {
            return null;
        }
    }

    protected Map<String, Object> getJobDataMap(ReadContext ctx) {
        if (this.jobDataPath != null) {
            return ctx.read(jobDataPath, MAP_TYPE_REF);
        } else {
            return Collections.emptyMap();
        }
    }

    protected Boolean getDurability(ReadContext ctx) {
        if (this.durablePath != null) {
            return ctx.read(durablePath, Boolean.class);
        } else {
            return null;
        }
    }

    protected List<Trigger> getTriggers(JobKey jobKey, ReadContext ctx) throws ParseException {

        Object cronScheduleTriggers = ctx.read(cronScheduleTriggerPath);
        ArrayList<Trigger> triggers = new ArrayList<>();

        if (cronScheduleTriggers != null) {
            triggers.addAll(getCronScheduleTriggers(jobKey, cronScheduleTriggers));
        }

        return triggers;
    }

    protected Class<? extends Job> getJobClass(ReadContext ctx) {
        return jobClass;
    }

    protected List<Trigger> getCronScheduleTriggers(JobKey jobKey, Object scheduleTriggers) throws ParseException {
        List<Trigger> result = new ArrayList<>();

        if (scheduleTriggers instanceof TextNode) {
            result.add(createCronTrigger(jobKey, ((TextNode) scheduleTriggers).textValue()));
        } else if (scheduleTriggers instanceof ArrayNode) {
            for (JsonNode trigger : (ArrayNode) scheduleTriggers) {
                String triggerDef = trigger.textValue();

                if (triggerDef != null) {
                    result.add(createCronTrigger(jobKey, triggerDef));
                }
            }
        }

        return result;
    }

    protected String getTriggerKey(String trigger) {
        return DigestUtils.md5Hex(trigger);
    }

    protected Trigger createCronTrigger(JobKey jobKey, String cronExpression) throws ParseException {
        String triggerKey = getTriggerKey(cronExpression);

        return TriggerBuilder.newTrigger().withIdentity(jobKey.getName() + "___" + triggerKey, group).forJob(jobKey)
                .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)).build();
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Class<? extends Job> getJobClass() {
        return jobClass;
    }

    public void setJobClass(Class<? extends Job> jobClass) {
        this.jobClass = jobClass;
    }

    public String getDescriptionPath() {
        return descriptionPath;
    }

    public void setDescriptionPath(String descriptionPath) {
        this.descriptionPath = descriptionPath;
    }

    public String getDurablePath() {
        return durablePath;
    }

    public void setDurablePath(String durablePath) {
        this.durablePath = durablePath;
    }

    public String getCronScheduleTriggerPath() {
        return cronScheduleTriggerPath;
    }

    public void setCronScheduleTriggerPath(String cronScheduleTriggerPath) {
        this.cronScheduleTriggerPath = cronScheduleTriggerPath;
    }

}
