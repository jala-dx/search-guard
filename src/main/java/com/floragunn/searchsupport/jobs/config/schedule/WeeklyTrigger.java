package com.floragunn.searchsupport.jobs.config.schedule;

import java.io.IOException;
import java.text.ParseException;
import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.DateBuilder;
import org.quartz.ScheduleBuilder;
import org.quartz.TimeOfDay;
import org.quartz.impl.triggers.CronTriggerImpl;

import com.fasterxml.jackson.databind.JsonNode;

public class WeeklyTrigger extends HumanReadableCronTrigger<WeeklyTrigger> {

    private static final long serialVersionUID = -8707981523995856355L;

    private List<DayOfWeek> on;
    private List<TimeOfDay> at;

    public WeeklyTrigger(List<DayOfWeek> on, List<TimeOfDay> at) {
        this.on = Collections.unmodifiableList(on);
        this.at = Collections.unmodifiableList(at);
        
        init();
    }

    @Override
    public ScheduleBuilder<WeeklyTrigger> getScheduleBuilder() {
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

    public List<DayOfWeek> getOn() {
        return on;
    }

    public void setOn(List<DayOfWeek> on) {
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
        builder.startObject("weekly");

        if (on.size() == 1) {
            builder.field("on", on.get(0).toString().toLowerCase());
        } else {
            builder.startArray("on");

            for (DayOfWeek dayOfWeek : on) {
                builder.value(dayOfWeek.toString().toLowerCase());
            }

            builder.endArray();
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

    public static WeeklyTrigger create(JsonNode jsonNode) throws ParseException {
        List<DayOfWeek> on;
        List<TimeOfDay> at;

        JsonNode onNode = jsonNode.get("on");

        if (onNode.isArray()) {
            on = new ArrayList<>(onNode.size());

            for (JsonNode onNodeElement : onNode) {
                on.add(getDayOfWeek(onNodeElement.textValue()));
            }
        } else if (onNode.isTextual()) {
            on = Collections.singletonList(getDayOfWeek(onNode.textValue()));
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

        return new WeeklyTrigger(on, at);
    }

    private static int toQuartz(DayOfWeek dayOfWeek) {
        if (dayOfWeek == DayOfWeek.SUNDAY) {
            return DateBuilder.SUNDAY;
        } else {
            return dayOfWeek.getValue() + 1;
        }
    }

    private String format(TimeOfDay timeOfDay) {
        StringBuilder result = new StringBuilder(timeOfDay.getHour());

        result.append(':');

        if (timeOfDay.getMinute() < 10) {
            result.append('0');
        }

        result.append(timeOfDay.getMinute());

        if (timeOfDay.getSecond() != 0) {
            result.append(':');

            if (timeOfDay.getSecond() < 10) {
                result.append('0');
            }

            result.append(timeOfDay.getSecond());
        }

        return result.toString();
    }

    private static CronExpression createCronExpression(TimeOfDay timeOfDay, List<DayOfWeek> on) {
        try {
            StringBuilder result = new StringBuilder();

            result.append(timeOfDay.getSecond()).append(' ');
            result.append(timeOfDay.getMinute()).append(' ');
            result.append(timeOfDay.getHour()).append(' ');
            result.append("? * ");

            if (on.size() == 0) {
                result.append("*");
            } else {
                boolean first = true;

                for (DayOfWeek dayOfWeek : on) {
                    if (first) {
                        first = false;
                    } else {
                        result.append(",");
                    }

                    result.append(toQuartz(dayOfWeek));
                }
            }

            return new CronExpression(result.toString());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private static TimeOfDay parseTimeOfDay(String string) throws ParseException {
        try {

            int colon = string.indexOf(':');

            if (colon == -1) {
                int hour = Integer.parseInt(string);

                return new TimeOfDay(hour, 0);
            } else {
                int hour = Integer.parseInt(string.substring(0, colon));
                int minute;
                int second = 0;

                int nextColon = string.indexOf(':', colon + 1);

                if (nextColon == -1) {
                    minute = Integer.parseInt(string.substring(colon + 1));
                } else {
                    minute = Integer.parseInt(string.substring(colon + 1, nextColon));
                    second = Integer.parseInt(string.substring(nextColon + 1));
                }

                return new TimeOfDay(hour, minute, second);
            }

        } catch (

        NumberFormatException e) {
            throw new ParseException("Illegal time format: " + string, -1);
        }
    }

    private static DayOfWeek getDayOfWeek(String string) throws ParseException {
        switch (string) {
        case "sunday":
        case "sun":
            return DayOfWeek.SUNDAY;
        case "monday":
        case "mon":
            return DayOfWeek.MONDAY;
        case "tuesday":
        case "tue":
            return DayOfWeek.TUESDAY;
        case "wednesday":
        case "wed":
            return DayOfWeek.WEDNESDAY;
        case "thursday":
        case "thu":
            return DayOfWeek.THURSDAY;
        case "friday":
        case "fri":
            return DayOfWeek.FRIDAY;
        case "saturday":
        case "sat":
            return DayOfWeek.SATURDAY;
        default:
            throw new ParseException("Illegal day of week: " + string, -1);
        }
    }
}
