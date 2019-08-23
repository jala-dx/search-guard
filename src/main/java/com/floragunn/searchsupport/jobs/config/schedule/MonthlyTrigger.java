package com.floragunn.searchsupport.jobs.config.schedule;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.ScheduleBuilder;
import org.quartz.TimeOfDay;
import org.quartz.impl.triggers.CronTriggerImpl;

import com.fasterxml.jackson.databind.JsonNode;

public class MonthlyTrigger extends HumanReadableCronTrigger<MonthlyTrigger> {

    private static final long serialVersionUID = -6518785696829462600L;
    private List<Integer> on;
    private List<TimeOfDay> at;

    public MonthlyTrigger(List<Integer> on, List<TimeOfDay> at, TimeZone timeZone) {
        this.on = Collections.unmodifiableList(on);
        this.at = Collections.unmodifiableList(at);
        this.timeZone = timeZone;

        init();
    }

    @Override
    public ScheduleBuilder<MonthlyTrigger> getScheduleBuilder() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected List<CronTriggerImpl> buildCronTriggers() {
        List<CronTriggerImpl> result = new ArrayList<>();

        for (TimeOfDay timeOfDay : at) {
            CronTriggerImpl cronTigger = (CronTriggerImpl) CronScheduleBuilder.cronSchedule(createCronExpression(timeOfDay, on)).build();

            result.add(cronTigger);
        }

        return result;
    }

    public List<Integer> getOn() {
        return on;
    }

    public void setOn(List<Integer> on) {
        this.on = on;
    }

    public List<TimeOfDay> getAt() {
        return at;
    }

    public void setAt(List<TimeOfDay> at) {
        this.at = at;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (on.size() == 1) {
            builder.field("on", on.get(0));
        } else {
            builder.array("on", on);
        }

        if (at.size() == 1) {
            builder.field("at", format(at.get(0)));
        } else {
            builder.startArray("at");

            for (TimeOfDay timeOfDay : at) {
                builder.value(format(timeOfDay));
            }

            builder.endArray();
        }

        builder.endObject();

        return builder;
    }

    public static MonthlyTrigger create(JsonNode jsonNode, TimeZone timeZone) throws ParseException {
        List<Integer> on;
        List<TimeOfDay> at;

        JsonNode onNode = jsonNode.get("on");

        if (onNode.isArray()) {
            on = new ArrayList<>(onNode.size());

            for (JsonNode onNodeElement : onNode) {
                on.add(onNodeElement.asInt());
            }
        } else if (onNode.isNumber()) {
            on = Collections.singletonList(onNode.asInt());
        } else {
            on = Collections.emptyList();
        }

        JsonNode atNode = jsonNode.get("at");

        if (atNode.isArray()) {
            at = new ArrayList<>(atNode.size());

            for (JsonNode atNodeElement : atNode) {
                at.add(parseTimeOfDay(atNodeElement.textValue()));
            }
        } else if (atNode.isTextual()) {
            at = Collections.singletonList(parseTimeOfDay(atNode.textValue()));
        } else {
            at = Collections.emptyList();
        }

        return new MonthlyTrigger(on, at, timeZone);
    }

    private static CronExpression createCronExpression(TimeOfDay timeOfDay, List<Integer> on) {
        try {
            StringBuilder result = new StringBuilder();

            result.append(timeOfDay.getSecond()).append(' ');
            result.append(timeOfDay.getMinute()).append(' ');
            result.append(timeOfDay.getHour()).append(' ');

            if (on.size() == 0) {
                result.append("*");
            } else {
                boolean first = true;

                for (Integer dayOfMonth : on) {
                    if (first) {
                        first = false;
                    } else {
                        result.append(",");
                    }

                    result.append(dayOfMonth);
                }
            }

            result.append(" * ?");

            return new CronExpression(result.toString());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

}
