package com.floragunn.searchsupport.jobs.config.schedule;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.elasticsearch.common.xcontent.ToXContentObject;
import org.quartz.Calendar;
import org.quartz.CronTrigger;
import org.quartz.Trigger;
import org.quartz.impl.triggers.AbstractTrigger;
import org.quartz.impl.triggers.CronTriggerImpl;

public abstract class HumanReadableCronTrigger<T extends Trigger> extends AbstractTrigger<T> implements Trigger, ToXContentObject {

    private static final long serialVersionUID = 8008213171103409490L;

    private List<CronTriggerImpl> generatedCronTriggers;
    private Date startTime = null;
    private Date endTime = null;
    private Date previousFireTime = null;

    protected abstract List<CronTriggerImpl> buildCronTriggers();

    protected void init() {
        this.generatedCronTriggers = buildCronTriggers();
    }

    @Override
    public void setNextFireTime(Date nextFireTime) {

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            delegate.setNextFireTime(nextFireTime);
        }
    }

    @Override
    public void setPreviousFireTime(Date previousFireTime) {
        this.previousFireTime = previousFireTime;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            delegate.setPreviousFireTime(previousFireTime);
        }
    }

    @Override
    public void triggered(Calendar calendar) {
        Date nextFireTime = getNextFireTime();
        previousFireTime = nextFireTime;

        nextFireTime = getFireTimeAfter(nextFireTime, calendar);

        // TODO only update one
        setNextFireTime(nextFireTime);
    }

    @Override
    public Date computeFirstFireTime(Calendar calendar) {
        Date result = null;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            Date delegateFirstFireTime = delegate.computeFirstFireTime(calendar);

            if (delegateFirstFireTime != null && (result == null || result.after(delegateFirstFireTime))) {
                result = delegateFirstFireTime;
            }
        }

        return result;
    }

    @Override
    public boolean mayFireAgain() {
        boolean result = false;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            result |= delegate.mayFireAgain();
        }

        return result;
    }

    @Override
    public Date getStartTime() {
        return startTime;
    }

    @Override
    public void setStartTime(Date startTime) {
        this.startTime = startTime;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            delegate.setStartTime(startTime);
        }
    }

    @Override
    public void setEndTime(Date endTime) {
        this.endTime = endTime;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            delegate.setEndTime(endTime);
        }
    }

    @Override
    public Date getEndTime() {
        return endTime;
    }

    @Override
    public Date getNextFireTime() {
        Date result = null;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            Date delegateNextFireTime = delegate.getNextFireTime();

            if (delegateNextFireTime != null && (result == null || result.after(delegateNextFireTime))) {
                result = delegateNextFireTime;
            }
        }

        return result;
    }

    @Override
    public Date getPreviousFireTime() {
        return previousFireTime;
    }

    @Override
    public Date getFireTimeAfter(Date afterTime) {
        Date result = null;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            Date delegateFireTimeAfter = delegate.getFireTimeAfter(afterTime);

            if (delegateFireTimeAfter != null && (result == null || result.after(delegateFireTimeAfter))) {
                result = delegateFireTimeAfter;
            }
        }

        return result;
    }

    @Override
    public Date getFinalFireTime() {
        Date result = null;

        for (CronTriggerImpl delegate : generatedCronTriggers) {
            Date delegateFinalFireTime = delegate.getFinalFireTime();

            if (delegateFinalFireTime != null && (result == null || result.before(delegateFinalFireTime))) {
                result = delegateFinalFireTime;
            }
        }

        return result;
    }

    @Override
    protected boolean validateMisfireInstruction(int candidateMisfireInstruction) {
        return candidateMisfireInstruction >= MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY
                && candidateMisfireInstruction <= CronTrigger.MISFIRE_INSTRUCTION_DO_NOTHING;
    }

    @Override
    public void updateAfterMisfire(Calendar cal) {
        int misfireInstruction = getMisfireInstruction();

        if (misfireInstruction == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) {
            return;
        }

        if (misfireInstruction == MISFIRE_INSTRUCTION_SMART_POLICY) {
            misfireInstruction = CronTrigger.MISFIRE_INSTRUCTION_FIRE_ONCE_NOW;
        }

        if (misfireInstruction == CronTrigger.MISFIRE_INSTRUCTION_DO_NOTHING) {
            Date now = new Date();

            for (CronTriggerImpl delegate : generatedCronTriggers) {
                delegate.setNextFireTime(getFireTimeAfter(delegate, now, cal));
            }
        } else if (misfireInstruction == CronTrigger.MISFIRE_INSTRUCTION_FIRE_ONCE_NOW) {

            Date now = new Date();
            CronTriggerImpl earliest = null;

            for (CronTriggerImpl delegate : generatedCronTriggers) {
                delegate.setNextFireTime(getFireTimeAfter(delegate, now, cal));

                if (earliest == null || earliest.getNextFireTime().after(delegate.getNextFireTime())) {
                    earliest = delegate;
                }
            }

            earliest.setNextFireTime(new Date());
        }
    }

    @Override
    public void updateWithNewCalendar(Calendar cal, long misfireThreshold) {
        for (CronTriggerImpl delegate : generatedCronTriggers) {
            delegate.updateWithNewCalendar(cal, misfireThreshold);
        }
    }

    private Date getFireTimeAfter(CronTriggerImpl trigger, Date afterTime, Calendar cal) {
        Date result = trigger.getFireTimeAfter(afterTime);
        while (result != null && cal != null && !cal.isTimeIncluded(result.getTime())) {
            result = trigger.getFireTimeAfter(result);
        }

        return result;
    }

    private Date getFireTimeAfter(Date afterTime, Calendar cal) {
        Date result = getFireTimeAfter(afterTime);
        while (result != null && cal != null && !cal.isTimeIncluded(result.getTime())) {
            result = getFireTimeAfter(result);
        }

        return result;
    }
}
