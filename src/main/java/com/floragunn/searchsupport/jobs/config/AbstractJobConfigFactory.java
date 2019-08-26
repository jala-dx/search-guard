package com.floragunn.searchsupport.jobs.config;

import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.codec.digest.DigestUtils;
import org.elasticsearch.common.bytes.BytesReference;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.floragunn.searchsupport.jobs.config.schedule.DailyTrigger;
import com.floragunn.searchsupport.jobs.config.schedule.MonthlyTrigger;
import com.floragunn.searchsupport.jobs.config.schedule.WeeklyTrigger;
import com.floragunn.searchsupport.jobs.config.validation.ConfigValidationException;
import com.floragunn.searchsupport.jobs.config.validation.InvalidAttributeValue;
import com.floragunn.searchsupport.jobs.config.validation.ValidationErrors;
import com.floragunn.searchsupport.util.DurationFormat;
import com.floragunn.searchsupport.util.JacksonTools;
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
    protected String dailyScheduleTriggerPath = "$.trigger.schedule.daily";
    protected String weeklyScheduleTriggerPath = "$.trigger.schedule.weekly";
    protected String monthlyScheduleTriggerPath = "$.trigger.schedule.monthly";
    protected String intervalScheduleTriggerPath = "$.trigger.schedule.interval";
    protected String timezonePath = "$.trigger.schedule.timezone";
    protected String authTokenPath = "$._meta.auth_token";
    protected String jobDataPath = "$";

    protected final static Configuration JSON_PATH_CONFIG = Configuration.builder().options(com.jayway.jsonpath.Option.SUPPRESS_EXCEPTIONS)
            .jsonProvider(new JacksonJsonNodeJsonProvider()).mappingProvider(new JacksonMappingProvider()).build();
    protected final static TypeRef<Map<String, Object>> MAP_TYPE_REF = new TypeRef<Map<String, Object>>() {
    };

    public AbstractJobConfigFactory(Class<? extends Job> jobClass) {
        this.jobClass = jobClass;
    }

    @Override
    public JobConfigType createFromBytes(String id, BytesReference source, long version) throws ConfigValidationException {
        ReadContext ctx = JsonPath.using(JSON_PATH_CONFIG).parse(source.utf8ToString());

        return createFromReadContext(id, ctx, version);
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

    abstract protected JobConfigType createFromReadContext(String id, ReadContext ctx, long version) throws ConfigValidationException;

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
            return JacksonTools.toMap(ctx.read(jobDataPath, JsonNode.class));
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

    protected List<Trigger> getTriggers(JobKey jobKey, ReadContext ctx) throws ConfigValidationException {
        ValidationErrors validationErrors = new ValidationErrors();

        ArrayList<Trigger> triggers = new ArrayList<>();

        String timeZoneId = ctx.read(timezonePath, String.class);
        TimeZone timeZone = null;

        if (timeZoneId != null) {
            timeZone = TimeZone.getTimeZone(timeZoneId);

            if (timeZone == null) {
                validationErrors.add(new InvalidAttributeValue("timezone", timeZoneId, TimeZone.class, null));
            }
        }

        Object cronScheduleTriggers = ctx.read(cronScheduleTriggerPath);

        if (cronScheduleTriggers != null) {
            try {
                triggers.addAll(getCronScheduleTriggers(jobKey, cronScheduleTriggers, timeZone));
            } catch (ConfigValidationException e) {
                validationErrors.add("cron", e);
            }
        }

        Object intervalScheduleTriggers = ctx.read(intervalScheduleTriggerPath);

        if (intervalScheduleTriggers != null) {
            try {
                triggers.addAll(getIntervalScheduleTriggers(jobKey, intervalScheduleTriggers));
            } catch (ConfigValidationException e) {
                validationErrors.add("interval", e);
            }
        }

        Object weeklyTriggers = ctx.read(weeklyScheduleTriggerPath);

        if (weeklyTriggers != null) {
            try {
                triggers.addAll(getWeeklyTriggers(jobKey, weeklyTriggers, timeZone));
            } catch (ConfigValidationException e) {
                validationErrors.add("weekly", e);
            }
        }

        Object dailyTriggers = ctx.read(dailyScheduleTriggerPath);

        if (dailyTriggers != null) {
            try {
                triggers.addAll(getDailyTriggers(jobKey, dailyTriggers, timeZone));
            } catch (ConfigValidationException e) {
                validationErrors.add("daily", e);
            }
        }

        Object monthlyTriggers = ctx.read(monthlyScheduleTriggerPath);

        if (monthlyTriggers != null) {
            try {
                triggers.addAll(getMonthlyTriggers(jobKey, monthlyTriggers, timeZone));
            } catch (ConfigValidationException e) {
                validationErrors.add("monthly", e);
            }
        }

        validationErrors.throwExceptionForPresentErrors();

        return triggers;
    }

    protected String getAuthToken(ReadContext ctx) {
        if (this.authTokenPath != null) {
            return ctx.read(authTokenPath, String.class);
        } else {
            return null;
        }
    }

    protected Class<? extends Job> getJobClass(ReadContext ctx) {
        return jobClass;
    }

    protected List<Trigger> getCronScheduleTriggers(JobKey jobKey, Object scheduleTriggers, TimeZone timeZone) throws ConfigValidationException {
        List<Trigger> result = new ArrayList<>();

        if (scheduleTriggers instanceof TextNode) {
            result.add(createCronTrigger(jobKey, ((TextNode) scheduleTriggers).textValue(), timeZone));
        } else if (scheduleTriggers instanceof ArrayNode) {
            for (JsonNode trigger : (ArrayNode) scheduleTriggers) {
                String triggerDef = trigger.textValue();

                if (triggerDef != null) {
                    result.add(createCronTrigger(jobKey, triggerDef, timeZone));
                }
            }
        }

        return result;
    }

    protected List<Trigger> getIntervalScheduleTriggers(JobKey jobKey, Object scheduleTriggers) throws ConfigValidationException {
        List<Trigger> result = new ArrayList<>();

        if (scheduleTriggers instanceof TextNode) {
            result.add(createIntervalScheduleTrigger(jobKey, ((TextNode) scheduleTriggers).textValue()));
        } else if (scheduleTriggers instanceof ArrayNode) {
            for (JsonNode trigger : (ArrayNode) scheduleTriggers) {
                String triggerDef = trigger.textValue();

                if (triggerDef != null) {
                    result.add(createIntervalScheduleTrigger(jobKey, triggerDef));
                }
            }
        }

        return result;
    }

    protected List<Trigger> getWeeklyTriggers(JobKey jobKey, Object scheduleTriggers, TimeZone timeZone) throws ConfigValidationException {
        List<Trigger> result = new ArrayList<>();

        if (scheduleTriggers instanceof ObjectNode) {
            result.add(createWeeklyTrigger(jobKey, (ObjectNode) scheduleTriggers, timeZone));
        } else if (scheduleTriggers instanceof ArrayNode) {
            for (JsonNode trigger : (ArrayNode) scheduleTriggers) {
                result.add(createWeeklyTrigger(jobKey, trigger, timeZone));
            }
        }

        return result;
    }

    protected List<Trigger> getMonthlyTriggers(JobKey jobKey, Object scheduleTriggers, TimeZone timeZone) throws ConfigValidationException {
        List<Trigger> result = new ArrayList<>();

        if (scheduleTriggers instanceof ObjectNode) {
            result.add(createMonthlyTrigger(jobKey, (ObjectNode) scheduleTriggers, timeZone));
        } else if (scheduleTriggers instanceof ArrayNode) {
            for (JsonNode trigger : (ArrayNode) scheduleTriggers) {
                result.add(createMonthlyTrigger(jobKey, trigger, timeZone));
            }
        }

        return result;
    }

    protected List<Trigger> getDailyTriggers(JobKey jobKey, Object scheduleTriggers, TimeZone timeZone) throws ConfigValidationException {
        List<Trigger> result = new ArrayList<>();

        if (scheduleTriggers instanceof ObjectNode) {
            result.add(createDailyTrigger(jobKey, (ObjectNode) scheduleTriggers, timeZone));
        } else if (scheduleTriggers instanceof ArrayNode) {
            for (JsonNode trigger : (ArrayNode) scheduleTriggers) {
                result.add(createDailyTrigger(jobKey, trigger, timeZone));
            }
        }

        return result;
    }

    protected String getTriggerKey(String trigger) {
        return DigestUtils.md5Hex(trigger);
    }

    protected Trigger createCronTrigger(JobKey jobKey, String cronExpression, TimeZone timeZone) throws ConfigValidationException {
        String triggerKey = getTriggerKey(cronExpression);

        try {
            return TriggerBuilder.newTrigger().withIdentity(jobKey.getName() + "___" + triggerKey, group).forJob(jobKey)
                    .withSchedule(CronScheduleBuilder.cronScheduleNonvalidatedExpression(cronExpression).inTimeZone(timeZone)).build();
        } catch (ParseException e) {
            throw new ConfigValidationException(new InvalidAttributeValue(null, cronExpression,
                    "Quartz Cron Expression: <Seconds: 0-59|*> <Minutes: 0-59|*> <Hours: 0-23|*> <Day-of-Month: 1-31|?|*> <Month: JAN-DEC|*> <Day-of-Week: SUN-SAT|?|*> <Year: 1970-2199|*>?. Numeric ranges: 1-2; Several distinct values: 1,2; Increments: 0/15",
                    null).message("Invalid cron expression: " + e.getMessage()).cause(e));
        }
    }

    protected Trigger createIntervalScheduleTrigger(JobKey jobKey, String interval) throws ConfigValidationException {
        String triggerKey = getTriggerKey(interval);
        Duration duration = DurationFormat.INSTANCE.parse(interval);

        return TriggerBuilder.newTrigger().withIdentity(jobKey.getName() + "___" + triggerKey, group).forJob(jobKey)
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().repeatForever().withIntervalInMilliseconds(duration.toMillis())).build();
    }

    protected Trigger createWeeklyTrigger(JobKey jobKey, JsonNode jsonNode, TimeZone timeZone) throws ConfigValidationException {
        String triggerKey = getTriggerKey(jsonNode.toString());

        WeeklyTrigger trigger = WeeklyTrigger.create(jsonNode, timeZone);
        trigger.setJobKey(jobKey);
        trigger.setKey(new TriggerKey(jobKey.getName() + "___" + triggerKey, group));

        return trigger;
    }

    protected Trigger createMonthlyTrigger(JobKey jobKey, JsonNode jsonNode, TimeZone timeZone) throws ConfigValidationException {
        String triggerKey = getTriggerKey(jsonNode.toString());

        MonthlyTrigger trigger = MonthlyTrigger.create(jsonNode, timeZone);
        trigger.setJobKey(jobKey);
        trigger.setKey(new TriggerKey(jobKey.getName() + "___" + triggerKey, group));

        return trigger;
    }

    protected Trigger createDailyTrigger(JobKey jobKey, JsonNode jsonNode, TimeZone timeZone) throws ConfigValidationException {
        String triggerKey = getTriggerKey(jsonNode.toString());

        DailyTrigger trigger = DailyTrigger.create(jsonNode, timeZone);
        trigger.setJobKey(jobKey);
        trigger.setKey(new TriggerKey(jobKey.getName() + "___" + triggerKey, group));

        return trigger;
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

    public String getIntervalScheduleTriggerPath() {
        return intervalScheduleTriggerPath;
    }

    public void setIntervalScheduleTriggerPath(String intervalScheduleTriggerPath) {
        this.intervalScheduleTriggerPath = intervalScheduleTriggerPath;
    }

}
