package com.floragunn.searchsupport.jobs.config;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.bytes.BytesReference;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.triggers.CronTriggerImpl;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.TypeRef;

public class DefaultJobConfigFactory implements JobConfigFactory<DefaultJobConfig> {

    private String group = "main";
    private Class<? extends Job> jobClass;
    private String descriptionPath = "$.description";
    private String durablePath = "$.durable";
    private String cronScheduleTriggerPath = "$.trigger.schedule.cron";
    private String jobDataPath = "$";
    private final static Configuration JSON_PATH_CONFIG = Configuration.builder().options(com.jayway.jsonpath.Option.SUPPRESS_EXCEPTIONS).build();

    public DefaultJobConfigFactory(Class<? extends Job> jobClass) {
        this.jobClass = jobClass;
    }

    @Override
    public DefaultJobConfig createFromBytes(String id, BytesReference source) throws ParseException {
        DefaultJobConfig result = new DefaultJobConfig(jobClass);
        ReadContext ctx = JsonPath.using(JSON_PATH_CONFIG).parse(source.utf8ToString());

        JobKey jobKey = new JobKey(id, group);
        result.setJobKey(jobKey);

        if (this.descriptionPath != null) {
            result.setDescription(ctx.read(descriptionPath, String.class));
        }

        if (this.durablePath != null) {
            Boolean durable = ctx.read(durablePath, Boolean.class);

            if (durable != null) {
                result.setDurable(durable);
            }
        }

        if (this.jobDataPath != null) {
            @SuppressWarnings("unchecked")
            Map<String, Object> jobDataMap = ctx.read(jobDataPath, Map.class);
            result.setJobDataMap(jobDataMap);
        }

        Object cronScheduleTriggers = ctx.read(cronScheduleTriggerPath);
        ArrayList<Trigger> triggers = new ArrayList<>();

        if (cronScheduleTriggers != null) {
            initCronScheduleTriggers(jobKey, ctx.read(cronScheduleTriggerPath), triggers);
        }

        result.setTriggers(triggers);

        return result;
    }

    private void initCronScheduleTriggers(JobKey jobKey, Object scheduleTriggers, ArrayList<Trigger> triggers) throws ParseException {
        if (scheduleTriggers instanceof String) {
            triggers.add(createCronTrigger(jobKey, "0", (String) scheduleTriggers));
        } else if (scheduleTriggers instanceof List) {
            int index = 0;

            for (Object trigger : ((List<?>) scheduleTriggers)) {
                triggers.add(createCronTrigger(jobKey, String.valueOf(index), String.valueOf(trigger)));

                index++;
            }

        }

    }

    private Trigger createCronTrigger(JobKey jobKey, String triggerId, String cronExpression) throws ParseException {
        return TriggerBuilder.newTrigger().withIdentity(jobKey.getName() + "___" + triggerId, group).forJob(jobKey)
                .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)).build();
    }

    @Override
    public JobDetail createJobDetail(DefaultJobConfig jobType) {
        JobBuilder jobBuilder = JobBuilder.newJob(jobType.getJobClass());

        jobBuilder.withIdentity(jobType.getJobKey());

        if (jobType.getJobDataMap() != null) {
            jobBuilder.setJobData(new JobDataMap(jobType.getJobDataMap()));
        }

        jobBuilder.withDescription(jobType.getDescription());
        jobBuilder.storeDurably(jobType.isDurable());
        return jobBuilder.build();
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
