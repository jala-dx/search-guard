package com.floragunn.searchsupport.jobs.config.schedule.units;

public class TimeOfDay {
    private final int hour;
    private final int minute;
    
    public TimeOfDay(int hour, int minute) {
        this.hour = hour;
        this.minute = minute;
    }

    public int getHour() {
        return hour;
    }

    public int getMinute() {
        return minute;
    }
}
