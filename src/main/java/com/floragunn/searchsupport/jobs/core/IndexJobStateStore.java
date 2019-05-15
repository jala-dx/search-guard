package com.floragunn.searchsupport.jobs.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.quartz.Calendar;
import org.quartz.DailyTimeIntervalTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.ScheduleBuilder;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.matchers.StringMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;

import com.floragunn.searchsupport.jobs.config.JobConfig;
import com.floragunn.searchsupport.jobs.config.JobConfigFactory;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

public class IndexJobStateStore<JobType extends com.floragunn.searchsupport.jobs.config.JobConfig> implements JobStore {
    private static final Logger log = LogManager.getLogger(IndexJobStateStore.class);

    private final String indexName;
    private final String statusIndexName;
    private final Client client;
    // private final JobFactory<JobType> jobFactory;
    private SchedulerSignaler signaler;
    private final Map<JobKey, StoredJobDetail> keyToJobMap = new HashMap<>();
    private final Map<TriggerKey, StoredOperableTrigger> keyToTriggerMap = new HashMap<>();
    private final Table<String, JobKey, StoredJobDetail> groupAndKeyToJobMap = HashBasedTable.create();
    private final Table<String, TriggerKey, StoredOperableTrigger> groupAndKeyToTriggerMap = HashBasedTable.create();
    private final TreeSet<StoredOperableTrigger> activeTriggers = new TreeSet<StoredOperableTrigger>(new Trigger.TriggerTimeComparator());
    private final Set<String> pausedTriggerGroups = new HashSet<String>();
    private final Set<String> pausedJobGroups = new HashSet<String>();
    private final Set<JobKey> blockedJobs = new HashSet<JobKey>();
    private final Iterable<JobType> jobConfigSource;
    private final JobConfigFactory<JobType> jobFactory;
    private boolean schedulerRunning = false;

    public IndexJobStateStore(String indexName, Client client, Iterable<JobType> jobConfigSource, JobConfigFactory<JobType> jobFactory) {
        this.indexName = indexName;
        this.statusIndexName = indexName + "_status";
        this.client = client;
        this.jobConfigSource = jobConfigSource;
        this.jobFactory = jobFactory;
    }

    @Override
    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {
        this.signaler = signaler;
        this.initJobs();
    }

    @Override
    public void schedulerStarted() throws SchedulerException {
        schedulerRunning = true;
    }

    @Override
    public void schedulerPaused() {
        schedulerRunning = false;
    }

    @Override
    public void schedulerResumed() {
        schedulerRunning = true;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public boolean supportsPersistence() {
        return true;
    }

    @Override
    public long getEstimatedTimeToReleaseAndAcquireTrigger() {
        return 50;
    }

    @Override
    public boolean isClustered() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger) throws ObjectAlreadyExistsException, JobPersistenceException {
        StoredOperableTrigger storedOperableTrigger;

        synchronized (this) {
            storeJob(newJob, false);
            storedOperableTrigger = storeTriggerInHeap(newTrigger, false);
        }

        setTriggerStatusInIndex(storedOperableTrigger);
    }

    @Override
    public synchronized void storeJob(JobDetail newJob, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
        StoredJobDetail newJobLocal = new StoredJobDetail(newJob);

        addToCollections(newJobLocal);
    }

    @Override
    public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
            throws ObjectAlreadyExistsException, JobPersistenceException {

        ArrayList<StoredOperableTrigger> storedOperableTriggers = new ArrayList<>();

        synchronized (this) {
            for (Entry<JobDetail, Set<? extends Trigger>> entry : triggersAndJobs.entrySet()) {
                storeJob(entry.getKey(), true);

                for (Trigger trigger : entry.getValue()) {
                    storedOperableTriggers.add(storeTriggerInHeap((OperableTrigger) trigger, true));
                }
            }
        }

        for (StoredOperableTrigger storedOperableTrigger : storedOperableTriggers) {
            setTriggerStatusInIndex(storedOperableTrigger);
        }
    }

    @Override
    public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
        StoredOperableTrigger storedOperableTrigger = storeTriggerInHeap(newTrigger, replaceExisting);

        setTriggerStatusInIndex(storedOperableTrigger);
    }

    @Override
    public synchronized boolean removeJob(JobKey jobKey) {
        boolean result = false;

        List<OperableTrigger> triggers = getTriggersForJob(jobKey);

        for (OperableTrigger trigger : triggers) {
            this.removeTrigger(trigger.getKey());
            result = true;
        }

        if (keyToJobMap.remove(jobKey) != null) {
            result = true;
        }

        groupAndKeyToJobMap.remove(jobKey.getGroup(), jobKey);

        return result;
    }

    @Override
    public synchronized boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
        boolean result = true;

        for (JobKey jobKey : jobKeys) {
            if (!removeJob(jobKey)) {
                result = false;
            }
        }
        return result;
    }

    @Override
    public synchronized boolean removeTrigger(TriggerKey triggerKey) {
        StoredOperableTrigger storedOperableTrigger = this.keyToTriggerMap.remove(triggerKey);

        if (storedOperableTrigger == null) {
            return false;
        }

        this.groupAndKeyToTriggerMap.remove(triggerKey.getGroup(), triggerKey);
        this.activeTriggers.remove(storedOperableTrigger);

        StoredJobDetail storedJobDetail = this.keyToJobMap.get(storedOperableTrigger.getJobKey());

        if (storedJobDetail != null) {
            storedJobDetail.triggers.remove(storedOperableTrigger);

            if (storedJobDetail.triggers.isEmpty()) {
                removeJob(storedJobDetail.getKey());
            }

        }

        return true;
    }

    @Override
    public synchronized boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        boolean result = true;

        for (TriggerKey triggerKey : triggerKeys) {
            if (!removeTrigger(triggerKey)) {
                result = false;
            }
        }
        return result;
    }

    @Override
    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
        StoredOperableTrigger storedOperableTrigger;

        synchronized (this) {
            storedOperableTrigger = this.keyToTriggerMap.get(triggerKey);

            if (storedOperableTrigger == null) {
                return false;
            }

            storedOperableTrigger.setDelegate(newTrigger);
            updateTriggerState(storedOperableTrigger);
        }

        setTriggerStatusInIndex(storedOperableTrigger);

        return true;
    }

    @Override
    public synchronized JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        return this.keyToJobMap.get(jobKey);
    }

    @Override
    public synchronized OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        return this.keyToTriggerMap.get(triggerKey);
    }

    @Override
    public synchronized boolean checkExists(JobKey jobKey) throws JobPersistenceException {
        return this.keyToJobMap.containsKey(jobKey);
    }

    @Override
    public synchronized boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
        return this.keyToTriggerMap.containsKey(triggerKey);
    }

    @Override
    public void clearAllSchedulingData() throws JobPersistenceException {
        keyToJobMap.clear();
        keyToTriggerMap.clear();
        groupAndKeyToJobMap.clear();
        groupAndKeyToTriggerMap.clear();
    }

    @Override
    public void storeCalendar(String name, Calendar calendar, boolean replaceExisting, boolean updateTriggers)
            throws ObjectAlreadyExistsException, JobPersistenceException {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean removeCalendar(String calName) throws JobPersistenceException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized int getNumberOfJobs() throws JobPersistenceException {
        return this.keyToJobMap.size();
    }

    @Override
    public synchronized int getNumberOfTriggers() throws JobPersistenceException {
        return this.keyToTriggerMap.size();
    }

    @Override
    public synchronized int getNumberOfCalendars() throws JobPersistenceException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public synchronized Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        if (matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            return Collections.unmodifiableSet(this.groupAndKeyToJobMap.row(matcher.getCompareToValue()).keySet());
        } else {
            HashSet<JobKey> result = new HashSet<>();
            StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
            String matcherValue = matcher.getCompareToValue();

            for (Map.Entry<String, Map<JobKey, StoredJobDetail>> entry : this.groupAndKeyToJobMap.rowMap().entrySet()) {
                if (operator.evaluate(entry.getKey(), matcherValue)) {
                    result.addAll(entry.getValue().keySet());
                }
            }

            return result;
        }
    }

    @Override
    public synchronized Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        if (matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            return Collections.unmodifiableSet(this.groupAndKeyToTriggerMap.row(matcher.getCompareToValue()).keySet());
        } else {
            HashSet<TriggerKey> result = new HashSet<>();
            StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
            String matcherValue = matcher.getCompareToValue();

            for (Map.Entry<String, Map<TriggerKey, StoredOperableTrigger>> entry : this.groupAndKeyToTriggerMap.rowMap().entrySet()) {
                if (operator.evaluate(entry.getKey(), matcherValue)) {
                    result.addAll(entry.getValue().keySet());
                }
            }

            return result;
        }
    }

    @Override
    public synchronized List<String> getJobGroupNames() throws JobPersistenceException {
        return new ArrayList<>(this.groupAndKeyToJobMap.rowKeySet());
    }

    @Override
    public synchronized List<String> getTriggerGroupNames() throws JobPersistenceException {
        return new ArrayList<>(this.groupAndKeyToTriggerMap.rowKeySet());
    }

    @Override
    public synchronized List<String> getCalendarNames() throws JobPersistenceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized List<OperableTrigger> getTriggersForJob(JobKey jobKey) {
        StoredJobDetail storedJobDetail = this.keyToJobMap.get(jobKey);

        if (storedJobDetail == null) {
            return null;
        }

        return storedJobDetail.triggers.stream().map(s -> s.delegate).collect(Collectors.toList());
    }

    @Override
    public synchronized TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
        StoredOperableTrigger storedOperableTrigger = this.keyToTriggerMap.get(triggerKey);

        if (storedOperableTrigger == null) {
            return TriggerState.NONE;
        } else {
            return storedOperableTrigger.state.getTriggerState();
        }
    }

    @Override
    public void resetTriggerFromErrorState(TriggerKey triggerKey) throws JobPersistenceException {
        StoredOperableTrigger storedOperableTrigger;

        synchronized (this) {
            storedOperableTrigger = this.keyToTriggerMap.get(triggerKey);

            if (storedOperableTrigger == null || storedOperableTrigger.state != StoredOperableTrigger.State.ERROR) {
                return;
            }

            storedOperableTrigger.state = StoredOperableTrigger.State.WAITING;
        }

        setTriggerStatusInIndex(storedOperableTrigger);

        updateTriggerState(storedOperableTrigger);
    }

    @Override
    public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        StoredOperableTrigger storedOperableTrigger;

        synchronized (this) {
            storedOperableTrigger = this.keyToTriggerMap.get(triggerKey);

            if (!pauseTriggerInHeap(storedOperableTrigger)) {
                return;
            }
        }

        setTriggerStatusInIndex(storedOperableTrigger);

    }

    @Override
    public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        Collection<StoredOperableTrigger> matchedTriggers;
        Collection<String> result;

        synchronized (this) {

            matchedTriggers = this.matchTriggers(matcher);

            if (matchedTriggers.isEmpty()) {
                return Collections.emptyList();
            }

            for (StoredOperableTrigger storedOperableTrigger : matchedTriggers) {
                pauseTriggerInHeap(storedOperableTrigger);
            }

            result = matchedTriggers.stream().map(t -> t.getKey().getGroup()).collect(Collectors.toSet());
        }

        for (StoredOperableTrigger storedOperableTrigger : matchedTriggers) {
            // TODO bulk
            setTriggerStatusInIndex(storedOperableTrigger);
        }

        return result;
    }

    @Override
    public void pauseJob(JobKey jobKey) throws JobPersistenceException {
        Collection<StoredOperableTrigger> changedTriggers;

        synchronized (this) {
            StoredJobDetail storedJobDetail = this.keyToJobMap.get(jobKey);

            if (storedJobDetail == null) {
                return;
            }

            changedTriggers = new ArrayList<>(storedJobDetail.triggers.size());

            for (StoredOperableTrigger storedOperableTrigger : storedJobDetail.triggers) {
                if (pauseTriggerInHeap(storedOperableTrigger)) {
                    changedTriggers.add(storedOperableTrigger);
                }
            }
        }

        for (StoredOperableTrigger storedOperableTrigger : changedTriggers) {
            // TODO bulk
            setTriggerStatusInIndex(storedOperableTrigger);
        }
    }

    @Override
    public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
        Collection<StoredJobDetail> matchedJobs;
        Collection<String> result;
        Collection<StoredOperableTrigger> changedTriggers;

        synchronized (this) {

            matchedJobs = this.matchJobs(groupMatcher);

            if (matchedJobs.isEmpty()) {
                return Collections.emptyList();
            }

            changedTriggers = new ArrayList<>(matchedJobs.size() * 3);

            for (StoredJobDetail storedJobDetail : matchedJobs) {
                for (StoredOperableTrigger storedOperableTrigger : storedJobDetail.triggers) {
                    if (pauseTriggerInHeap(storedOperableTrigger)) {
                        changedTriggers.add(storedOperableTrigger);
                    }
                }
            }

            result = matchedJobs.stream().map(t -> t.getKey().getGroup()).collect(Collectors.toSet());
        }

        for (StoredOperableTrigger storedOperableTrigger : changedTriggers) {
            // TODO bulk
            setTriggerStatusInIndex(storedOperableTrigger);
        }

        return result;
    }

    @Override
    public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        StoredOperableTrigger storedOperableTrigger;

        synchronized (this) {
            storedOperableTrigger = this.keyToTriggerMap.get(triggerKey);

            if (!resumeTriggerInHeap(storedOperableTrigger)) {
                return;
            }

        }

        setTriggerStatusInIndex(storedOperableTrigger);
    }

    @Override
    public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        Collection<StoredOperableTrigger> matchedTriggers;
        Collection<String> result;

        synchronized (this) {

            matchedTriggers = this.matchTriggers(matcher);

            if (matchedTriggers.isEmpty()) {
                return Collections.emptyList();
            }

            for (StoredOperableTrigger storedOperableTrigger : matchedTriggers) {
                resumeTriggerInHeap(storedOperableTrigger);
            }

            result = matchedTriggers.stream().map(t -> t.getKey().getGroup()).collect(Collectors.toSet());
        }

        for (StoredOperableTrigger storedOperableTrigger : matchedTriggers) {
            // TODO bulk
            setTriggerStatusInIndex(storedOperableTrigger);
        }

        return result;
    }

    @Override
    public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void resumeJob(JobKey jobKey) throws JobPersistenceException {
        Collection<StoredOperableTrigger> changedTriggers;

        synchronized (this) {
            StoredJobDetail storedJobDetail = this.keyToJobMap.get(jobKey);

            if (storedJobDetail == null) {
                return;
            }

            changedTriggers = new ArrayList<>(storedJobDetail.triggers.size());

            for (StoredOperableTrigger storedOperableTrigger : storedJobDetail.triggers) {
                if (resumeTriggerInHeap(storedOperableTrigger)) {
                    changedTriggers.add(storedOperableTrigger);
                }
            }
        }

        for (StoredOperableTrigger storedOperableTrigger : changedTriggers) {
            // TODO bulk
            setTriggerStatusInIndex(storedOperableTrigger);
        }
    }

    @Override
    public Collection<String> resumeJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
        Collection<StoredJobDetail> matchedJobs;
        Collection<String> result;
        Collection<StoredOperableTrigger> changedTriggers;

        synchronized (this) {

            matchedJobs = this.matchJobs(groupMatcher);

            if (matchedJobs.isEmpty()) {
                return Collections.emptyList();
            }

            changedTriggers = new ArrayList<>(matchedJobs.size() * 3);

            for (StoredJobDetail storedJobDetail : matchedJobs) {
                for (StoredOperableTrigger storedOperableTrigger : storedJobDetail.triggers) {
                    if (resumeTriggerInHeap(storedOperableTrigger)) {
                        changedTriggers.add(storedOperableTrigger);
                    }
                }
            }

            result = matchedJobs.stream().map(t -> t.getKey().getGroup()).collect(Collectors.toSet());
        }

        for (StoredOperableTrigger storedOperableTrigger : changedTriggers) {
            // TODO bulk
            setTriggerStatusInIndex(storedOperableTrigger);
        }

        return result;
    }

    @Override
    public void pauseAll() throws JobPersistenceException {
        Collection<StoredOperableTrigger> changedTriggers = new ArrayList<StoredOperableTrigger>();

        synchronized (this) {
            for (StoredOperableTrigger storedOperableTrigger : this.keyToTriggerMap.values()) {
                if (pauseTriggerInHeap(storedOperableTrigger)) {
                    changedTriggers.add(storedOperableTrigger);
                }
            }
        }

        for (StoredOperableTrigger storedOperableTrigger : changedTriggers) {
            // TODO bulk
            setTriggerStatusInIndex(storedOperableTrigger);
        }
    }

    @Override
    public void resumeAll() throws JobPersistenceException {
        Collection<StoredOperableTrigger> changedTriggers = new ArrayList<StoredOperableTrigger>();

        synchronized (this) {
            for (StoredOperableTrigger storedOperableTrigger : this.keyToTriggerMap.values()) {
                if (resumeTriggerInHeap(storedOperableTrigger)) {
                    changedTriggers.add(storedOperableTrigger);
                }
            }
        }

        for (StoredOperableTrigger storedOperableTrigger : changedTriggers) {
            // TODO bulk
            setTriggerStatusInIndex(storedOperableTrigger);
        }
    }

    @Override
    public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow) throws JobPersistenceException {
        List<OperableTrigger> result;

        synchronized (this) {
            if (this.activeTriggers.isEmpty()) {
                return Collections.emptyList();
            }

            result = new ArrayList<OperableTrigger>(Math.min(maxCount, this.activeTriggers.size()));
            Set<JobKey> acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();
            Set<StoredOperableTrigger> excludedTriggers = new HashSet<StoredOperableTrigger>();

            long batchEnd = noLaterThan;

            for (StoredOperableTrigger trigger = activeTriggers.pollFirst(); trigger != null
                    && result.size() < maxCount; trigger = activeTriggers.pollFirst()) {
                if (trigger.getNextFireTime() == null) {
                    continue;
                }

                /*
                 * 
                 * TODO
                if (applyMisfire(trigger)) {
                if (tw.trigger.getNextFireTime() != null) {
                    timeTriggers.add(tw);
                }
                continue;
                }
                */

                if (trigger.getNextFireTime().getTime() > batchEnd) {
                    activeTriggers.add(trigger);
                    break;
                }

                StoredJobDetail storedJobDetail = keyToJobMap.get(trigger.getJobKey());

                if (storedJobDetail.isConcurrentExectionDisallowed()) {
                    if (acquiredJobKeysForNoConcurrentExec.contains(storedJobDetail.getKey())) {
                        excludedTriggers.add(trigger);
                        continue;
                    } else {
                        acquiredJobKeysForNoConcurrentExec.add(trigger.getJobKey());
                    }
                }

                trigger.state = StoredOperableTrigger.State.ACQUIRED;
                // tw.trigger.setFireInstanceId(getFiredTriggerRecordId()); TODO

                if (result.isEmpty()) {
                    batchEnd = Math.max(trigger.getNextFireTime().getTime(), System.currentTimeMillis()) + timeWindow;
                }

                result.add(trigger);
            }

            this.activeTriggers.addAll(excludedTriggers);
        }

        for (OperableTrigger trigger : result) {
            // TODO batch
            setTriggerStatusInIndex((StoredOperableTrigger) trigger);
        }

        return result;
    }

    @Override
    public void releaseAcquiredTrigger(OperableTrigger trigger) {
        StoredOperableTrigger storedOperableTrigger;

        synchronized (this) {
            storedOperableTrigger = this.keyToTriggerMap.get(trigger.getKey());

            if (storedOperableTrigger == null) {
                return;
            }

            if (storedOperableTrigger.state != StoredOperableTrigger.State.ACQUIRED) {
                return;
            }

            storedOperableTrigger.state = StoredOperableTrigger.State.WAITING;

            activeTriggers.add(storedOperableTrigger);
        }

        try {

            setTriggerStatusInIndex(storedOperableTrigger);
        } catch (Exception e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> firedTriggers) throws JobPersistenceException {
        List<TriggerFiredResult> results = new ArrayList<TriggerFiredResult>(firedTriggers.size());
        Set<StoredOperableTrigger> changedTriggers = new HashSet<StoredOperableTrigger>();

        synchronized (this) {

            for (OperableTrigger trigger : firedTriggers) {
                StoredOperableTrigger storedOperableTrigger = this.keyToTriggerMap.get(trigger.getKey());

                if (storedOperableTrigger == null) {
                    continue;
                }

                // was the trigger completed, paused, blocked, etc. since being acquired?
                if (storedOperableTrigger.state != StoredOperableTrigger.State.ACQUIRED) {
                    continue;
                }

                Date prevFireTime = trigger.getPreviousFireTime();

                activeTriggers.remove(storedOperableTrigger);
                storedOperableTrigger.getDelegate().triggered(null);
                trigger.triggered(null);

                storedOperableTrigger.state = StoredOperableTrigger.State.WAITING;
                changedTriggers.add(storedOperableTrigger);

                StoredJobDetail jobDetail = this.keyToJobMap.get(trigger.getJobKey());

                TriggerFiredBundle triggerFiredBundle = new TriggerFiredBundle(jobDetail, trigger, null, false, new Date(),
                        trigger.getPreviousFireTime(), prevFireTime, trigger.getNextFireTime());

                if (jobDetail.isConcurrentExectionDisallowed()) {
                    // TODO verstehen und schöner
                    for (StoredOperableTrigger ttw : jobDetail.triggers) {
                        if (ttw.state == StoredOperableTrigger.State.WAITING) {
                            ttw.state = StoredOperableTrigger.State.BLOCKED;
                            changedTriggers.add(ttw);
                        } else if (ttw.state == StoredOperableTrigger.State.PAUSED) {
                            ttw.state = StoredOperableTrigger.State.PAUSED_BLOCKED;
                            changedTriggers.add(ttw);
                        }

                        this.activeTriggers.remove(ttw);
                    }
                    blockedJobs.add(jobDetail.getKey());
                } else if (storedOperableTrigger.getNextFireTime() != null) {
                    this.activeTriggers.add(storedOperableTrigger);
                }

                results.add(new TriggerFiredResult(triggerFiredBundle));
            }
        }

        for (StoredOperableTrigger trigger : changedTriggers) {
            // TODO batch
            setTriggerStatusInIndex(trigger);
        }

        return results;

    }

    @Override
    public void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail, CompletedExecutionInstruction triggerInstCode) {
        Set<StoredOperableTrigger> changedTriggers = new HashSet<>();
        synchronized (this) {
            StoredJobDetail storedJobDetail = this.keyToJobMap.get(jobDetail.getKey());
            StoredOperableTrigger storedOperableTrigger = this.keyToTriggerMap.get(trigger.getKey());

            if (storedJobDetail != null) {
                if (storedJobDetail.isPersistJobDataAfterExecution()) {
                    JobDataMap newData = jobDetail.getJobDataMap();
                    if (newData != null) {
                        newData = (JobDataMap) newData.clone();
                        newData.clearDirtyFlag();
                    }
                    // TODO persist?!
                    storedJobDetail.delegate = storedJobDetail.getJobBuilder().setJobData(newData).build();
                }

                if (storedJobDetail.isConcurrentExectionDisallowed()) {
                    blockedJobs.remove(storedJobDetail.getKey());

                    for (StoredOperableTrigger ttw : storedJobDetail.triggers) {
                        if (ttw.state == StoredOperableTrigger.State.BLOCKED) {
                            ttw.state = StoredOperableTrigger.State.WAITING;
                            this.activeTriggers.add(ttw);
                            changedTriggers.add(ttw);
                        } else if (ttw.state == StoredOperableTrigger.State.PAUSED_BLOCKED) {
                            ttw.state = StoredOperableTrigger.State.PAUSED;
                            changedTriggers.add(ttw);
                        }
                    }
                    signaler.signalSchedulingChange(0L);
                }
            } else {
                blockedJobs.remove(jobDetail.getKey());
            }

            if (storedOperableTrigger != null) {
                switch (triggerInstCode) {
                case DELETE_TRIGGER:
                    // TODO sinnvoll bei extern geladener config?
                    if (trigger.getNextFireTime() == null) {
                        // double check for possible reschedule within job 
                        // execution, which would cancel the need to delete...
                        if (storedOperableTrigger.getNextFireTime() == null) {
                            removeTrigger(storedOperableTrigger.getKey());
                        }
                    } else {
                        removeTrigger(trigger.getKey());
                        signaler.signalSchedulingChange(0L);
                    }
                    break;
                case SET_TRIGGER_COMPLETE:
                    storedOperableTrigger.state = StoredOperableTrigger.State.COMPLETE;
                    this.activeTriggers.remove(storedOperableTrigger);
                    changedTriggers.add(storedOperableTrigger);
                    signaler.signalSchedulingChange(0L);
                    break;
                case SET_TRIGGER_ERROR:
                    storedOperableTrigger.state = StoredOperableTrigger.State.ERROR;
                    this.activeTriggers.remove(storedOperableTrigger);
                    signaler.signalSchedulingChange(0L);
                    changedTriggers.add(storedOperableTrigger);
                    break;
                case SET_ALL_JOB_TRIGGERS_ERROR:
                    changedTriggers.addAll(setAllTriggersOfJobToState(storedJobDetail, StoredOperableTrigger.State.ERROR));
                    signaler.signalSchedulingChange(0L);
                    break;
                case SET_ALL_JOB_TRIGGERS_COMPLETE:
                    changedTriggers.addAll(setAllTriggersOfJobToState(storedJobDetail, StoredOperableTrigger.State.COMPLETE));
                    signaler.signalSchedulingChange(0L);
                    break;
                case NOOP:
                    break;
                case RE_EXECUTE_JOB:
                    break;
                default:
                    break;
                }
            }
        }

        for (StoredOperableTrigger storedOperableTrigger : changedTriggers) {
            // TODO batch
            try {
                setTriggerStatusInIndex(storedOperableTrigger);
            } catch (JobPersistenceException e) {
                // TODO
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void setInstanceId(String schedInstId) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setInstanceName(String schedName) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setThreadPoolSize(int poolSize) {
        // TODO Auto-generated method stub

    }

    @Override
    public long getAcquireRetryDelay(int failureCount) {
        return 20;
    }

    private synchronized StoredOperableTrigger storeTriggerInHeap(OperableTrigger newTrigger, boolean replaceExisting)
            throws ObjectAlreadyExistsException, JobPersistenceException {

        StoredJobDetail storedJobDetail = this.keyToJobMap.get(newTrigger.getJobKey());

        if (storedJobDetail == null) {
            throw new JobPersistenceException("Trigger " + newTrigger + " references non-existing job" + newTrigger.getJobKey());
        }

        StoredOperableTrigger storedOperableTrigger = new StoredOperableTrigger(newTrigger);

        storedJobDetail.addTrigger(storedOperableTrigger);

        addToCollections(storedOperableTrigger);

        updateTriggerState(storedOperableTrigger);

        return storedOperableTrigger;
    }

    private synchronized void addToCollections(StoredJobDetail storedJobDetail) {
        keyToJobMap.put(storedJobDetail.getKey(), storedJobDetail);
        groupAndKeyToJobMap.put(storedJobDetail.getKey().getGroup(), storedJobDetail.getKey(), storedJobDetail);

        if (!storedJobDetail.triggers.isEmpty()) {
            for (StoredOperableTrigger storedOperableTrigger : storedJobDetail.triggers) {
                this.addToCollections(storedOperableTrigger);
            }
        }
    }

    private synchronized void addToCollections(StoredOperableTrigger storedOperableTrigger) {
        this.groupAndKeyToTriggerMap.put(storedOperableTrigger.getKey().getGroup(), storedOperableTrigger.getKey(), storedOperableTrigger);
        this.keyToTriggerMap.put(storedOperableTrigger.getKey(), storedOperableTrigger);
    }

    private synchronized void updateAllTriggerStates() {
        for (StoredOperableTrigger trigger : this.keyToTriggerMap.values()) {
            // XXX
            trigger.computeFirstFireTime(null);
            updateTriggerState(trigger);
        }
    }

    private synchronized void updateTriggerState(StoredOperableTrigger storedOperableTrigger) {
        if (pausedTriggerGroups.contains(storedOperableTrigger.getKey().getGroup())
                || pausedJobGroups.contains(storedOperableTrigger.getJobKey().getGroup())) {
            if (blockedJobs.contains(storedOperableTrigger.getJobKey())) {
                storedOperableTrigger.state = StoredOperableTrigger.State.PAUSED_BLOCKED;
            } else {
                storedOperableTrigger.state = StoredOperableTrigger.State.PAUSED;
            }
            activeTriggers.remove(storedOperableTrigger);
        } else if (blockedJobs.contains(storedOperableTrigger.getJobKey())) {
            storedOperableTrigger.state = StoredOperableTrigger.State.BLOCKED;
            activeTriggers.remove(storedOperableTrigger);
        } else {
            storedOperableTrigger.state = StoredOperableTrigger.State.WAITING;
            activeTriggers.add(storedOperableTrigger);
        }
    }

    private void setTriggerStatusInIndex(StoredOperableTrigger storedOperableTrigger) throws JobPersistenceException {
        try {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
            storedOperableTrigger.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);

            IndexRequest indexRequest = new IndexRequest(statusIndexName).id(storedOperableTrigger.getKeyString()).source(xContentBuilder);

            // TODO change to async? https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-document-index.html
            IndexResponse indexResponse = this.client.index(indexRequest).get();
        } catch (IOException | InterruptedException | ExecutionException e) {
            throw new JobPersistenceException(e.getMessage(), e);
        }
    }

    private synchronized Collection<StoredOperableTrigger> matchTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        if (matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            return this.groupAndKeyToTriggerMap.row(matcher.getCompareToValue()).values();
        } else {
            HashSet<StoredOperableTrigger> result = new HashSet<>();
            StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
            String matcherValue = matcher.getCompareToValue();

            for (Map.Entry<String, Map<TriggerKey, StoredOperableTrigger>> entry : this.groupAndKeyToTriggerMap.rowMap().entrySet()) {
                if (operator.evaluate(entry.getKey(), matcherValue)) {
                    result.addAll(entry.getValue().values());
                }
            }

            return result;
        }
    }

    private synchronized Collection<StoredJobDetail> matchJobs(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        if (matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            return this.groupAndKeyToJobMap.row(matcher.getCompareToValue()).values();
        } else {
            HashSet<StoredJobDetail> result = new HashSet<>();
            StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
            String matcherValue = matcher.getCompareToValue();

            for (Map.Entry<String, Map<JobKey, StoredJobDetail>> entry : this.groupAndKeyToJobMap.rowMap().entrySet()) {
                if (operator.evaluate(entry.getKey(), matcherValue)) {
                    result.addAll(entry.getValue().values());
                }
            }

            return result;
        }
    }

    private synchronized boolean pauseTriggerInHeap(StoredOperableTrigger storedOperableTrigger) {
        if (storedOperableTrigger == null || storedOperableTrigger.state == StoredOperableTrigger.State.COMPLETE) {
            return false;
        }

        if (storedOperableTrigger.state == StoredOperableTrigger.State.BLOCKED) {
            storedOperableTrigger.state = StoredOperableTrigger.State.PAUSED_BLOCKED;
        } else {
            storedOperableTrigger.state = StoredOperableTrigger.State.PAUSED;
        }

        activeTriggers.remove(storedOperableTrigger);

        return true;
    }

    private synchronized boolean resumeTriggerInHeap(StoredOperableTrigger storedOperableTrigger) {
        if (storedOperableTrigger == null) {
            return false;
        }

        if (storedOperableTrigger.state != StoredOperableTrigger.State.PAUSED
                && storedOperableTrigger.state != StoredOperableTrigger.State.PAUSED_BLOCKED) {
            return false;
        }

        storedOperableTrigger.state = StoredOperableTrigger.State.WAITING;

        // TODO applyMisfire(tw);

        updateTriggerState(storedOperableTrigger);

        return true;
    }

    private Collection<StoredOperableTrigger> setAllTriggersOfJobToState(StoredJobDetail storedJobDetail, StoredOperableTrigger.State state) {
        Collection<StoredOperableTrigger> changedTriggers = new ArrayList<>(storedJobDetail.triggers.size());

        for (StoredOperableTrigger storedOperableTrigger : storedJobDetail.triggers) {
            if (storedOperableTrigger.state == state) {
                continue;
            }

            storedOperableTrigger.state = state;

            if (state != StoredOperableTrigger.State.WAITING) {
                this.activeTriggers.remove(storedOperableTrigger);
            }

            changedTriggers.add(storedOperableTrigger);
        }

        return changedTriggers;
    }

    private void initJobs() {
        Collection<StoredJobDetail> jobs = this.loadJobs();

        synchronized (this) {
            for (StoredJobDetail job : jobs) {
                addToCollections(job);
            }

            updateAllTriggerStates();
        }
    }

    private Collection<StoredJobDetail> loadJobs() {
        Set<JobType> jobConfigSet = this.loadJobConfig();

        if (log.isDebugEnabled()) {
            log.debug("Job configurations loaded: " + jobConfigSet);
        }

        Map<TriggerKey, StoredOperableTrigger> triggerStates = this.loadTriggerStates(jobConfigSet);
        Collection<StoredJobDetail> result = new ArrayList<>(jobConfigSet.size());

        for (JobType jobConfig : jobConfigSet) {
            StoredJobDetail storedJobDetail = new StoredJobDetail(this.jobFactory.createJobDetail(jobConfig));

            for (Trigger triggerConfig : jobConfig.getTriggers()) {
                if (!(triggerConfig instanceof OperableTrigger)) {
                    log.error("Trigger is not OperableTrigger: " + triggerConfig);
                    continue;
                }

                OperableTrigger operableTriggerConfig = (OperableTrigger) triggerConfig;
                StoredOperableTrigger storedOperableTrigger = triggerStates.get(triggerConfig.getKey());

                if (storedOperableTrigger != null) {
                    storedOperableTrigger.setDelegate(operableTriggerConfig);
                } else {
                    storedOperableTrigger = new StoredOperableTrigger(operableTriggerConfig);
                }

                storedJobDetail.addTrigger(storedOperableTrigger);
            }

            result.add(storedJobDetail);
        }

        if (log.isInfoEnabled()) {
            log.info("Jobs loaded: " + result);
        }

        return result;
    }

    private Map<TriggerKey, StoredOperableTrigger> loadTriggerStates(Set<JobType> jobConfig) {
        try {
            Map<String, TriggerKey> triggerIds = this.getTriggerIds(jobConfig);

            if (triggerIds.isEmpty()) {
                return Collections.emptyMap();
            }

            Map<TriggerKey, StoredOperableTrigger> result = new HashMap<>(triggerIds.size());

            QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds(triggerIds.keySet().toArray(new String[triggerIds.size()]));

            SearchResponse searchResponse = client.prepareSearch(this.statusIndexName).setQuery(queryBuilder).get();

            for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                try {
                    StoredOperableTrigger storedOperableTrigger = StoredOperableTrigger.fromAttributeMap(triggerIds.get(searchHit.getId()),
                            searchHit.getSourceAsMap());

                    result.put(storedOperableTrigger.getKey(), storedOperableTrigger);

                } catch (Exception e) {
                    log.error("Error while loading " + searchHit, e);
                }
            }

            // TODO scroll?!

            return result;
        } catch (IndexNotFoundException e) {
            return Collections.emptyMap();
        }
    }

    private Map<String, TriggerKey> getTriggerIds(Set<JobType> jobConfig) {
        Map<String, TriggerKey> result = new HashMap<String, TriggerKey>(jobConfig.size() * 3);

        for (JobConfig job : jobConfig) {
            for (Trigger trigger : job.getTriggers()) {
                result.put(quartzKeyToKeyString(trigger.getKey()), trigger.getKey());
            }
        }

        return result;
    }

    private Set<JobType> loadJobConfig() {
        return Sets.newHashSet(this.jobConfigSource);
    }

    private static String quartzKeyToKeyString(org.quartz.utils.Key<?> key) {
        return key.getGroup().replaceAll("\\\\", "\\\\").replaceAll("\\.", "\\.") + "."
                + key.getName().replaceAll("\\\\", "\\\\").replaceAll("\\.", "\\.");
    }

    static class StoredJobDetail implements JobDetail {

        private static final long serialVersionUID = -4500332272991179774L;

        private JobDetail delegate;
        private List<StoredOperableTrigger> triggers = new ArrayList<>();

        StoredJobDetail(JobDetail jobDetail) {
            this.delegate = jobDetail;
        }

        public void addTrigger(StoredOperableTrigger trigger) {
            this.triggers.add(trigger);
        }

        public JobKey getKey() {
            return delegate.getKey();
        }

        public String getDescription() {
            return delegate.getDescription();
        }

        public Class<? extends Job> getJobClass() {
            return delegate.getJobClass();
        }

        public JobDataMap getJobDataMap() {
            return delegate.getJobDataMap();
        }

        public boolean isDurable() {
            return delegate.isDurable();
        }

        public boolean isPersistJobDataAfterExecution() {
            return delegate.isPersistJobDataAfterExecution();
        }

        public boolean isConcurrentExectionDisallowed() {
            return delegate.isConcurrentExectionDisallowed();
        }

        public boolean requestsRecovery() {
            return delegate.requestsRecovery();
        }

        public JobBuilder getJobBuilder() {
            return delegate.getJobBuilder();
        }

        @Override
        public Object clone() {
            return new StoredJobDetail(this.delegate);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            JobKey key = getKey();
            int result = 1;
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (!(obj instanceof JobDetail))
                return false;
            JobDetail other = (JobDetail) obj;
            JobKey key = getKey();

            if (key == null) {
                if (other.getKey() != null)
                    return false;
            } else if (!key.equals(other.getKey()))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "StoredJobDetail [getKey()=" + getKey() + ", getDescription()=" + getDescription() + ", getJobClass()=" + getJobClass()
                    + ", getJobDataMap()=" + getJobDataMap() + ", isDurable()=" + isDurable() + ", isPersistJobDataAfterExecution()="
                    + isPersistJobDataAfterExecution() + ", isConcurrentExectionDisallowed()=" + isConcurrentExectionDisallowed()
                    + ", requestsRecovery()=" + requestsRecovery() + "]";
        }

    }

    static class StoredOperableTrigger implements OperableTrigger, ToXContentObject {
        private static final long serialVersionUID = -181071146931763579L;
        private OperableTrigger delegate;
        private final TriggerKey key;
        private final String keyString;
        private State state = State.WAITING;
        private String stateInfo = null;

        StoredOperableTrigger(TriggerKey key) {
            this.key = key;
            this.keyString = quartzKeyToKeyString(key);
        }

        StoredOperableTrigger(OperableTrigger operableTrigger) {
            this(operableTrigger.getKey());
            this.delegate = operableTrigger;
        }

        public void triggered(Calendar calendar) {
            delegate.triggered(calendar);
        }

        public Date computeFirstFireTime(Calendar calendar) {
            return delegate.computeFirstFireTime(calendar);
        }

        public void setKey(TriggerKey key) {
            delegate.setKey(key);
        }

        public void setJobKey(JobKey key) {
            delegate.setJobKey(key);
        }

        public void setDescription(String description) {
            delegate.setDescription(description);
        }

        public void setCalendarName(String calendarName) {
            delegate.setCalendarName(calendarName);
        }

        public CompletedExecutionInstruction executionComplete(JobExecutionContext context, JobExecutionException result) {
            return delegate.executionComplete(context, result);
        }

        public void setJobDataMap(JobDataMap jobDataMap) {
            delegate.setJobDataMap(jobDataMap);
        }

        public void setPriority(int priority) {
            delegate.setPriority(priority);
        }

        public void setStartTime(Date startTime) {
            delegate.setStartTime(startTime);
        }

        public void updateAfterMisfire(Calendar cal) {
            delegate.updateAfterMisfire(cal);
        }

        public void setEndTime(Date endTime) {
            delegate.setEndTime(endTime);
        }

        public void updateWithNewCalendar(Calendar cal, long misfireThreshold) {
            delegate.updateWithNewCalendar(cal, misfireThreshold);
        }

        public void setMisfireInstruction(int misfireInstruction) {
            delegate.setMisfireInstruction(misfireInstruction);
        }

        public void validate() throws SchedulerException {
            delegate.validate();
        }

        public Object clone() {
            return new StoredOperableTrigger(delegate);
        }

        public void setFireInstanceId(String id) {
            delegate.setFireInstanceId(id);
        }

        public String getFireInstanceId() {
            return delegate.getFireInstanceId();
        }

        public void setNextFireTime(Date nextFireTime) {
            delegate.setNextFireTime(nextFireTime);
        }

        public void setPreviousFireTime(Date previousFireTime) {
            delegate.setPreviousFireTime(previousFireTime);
        }

        public TriggerKey getKey() {
            return delegate.getKey();
        }

        public JobKey getJobKey() {
            return delegate.getJobKey();
        }

        public String getDescription() {
            return delegate.getDescription();
        }

        public String getCalendarName() {
            return delegate.getCalendarName();
        }

        public JobDataMap getJobDataMap() {
            return delegate.getJobDataMap();
        }

        public int getPriority() {
            return delegate.getPriority();
        }

        public boolean mayFireAgain() {
            return delegate.mayFireAgain();
        }

        public Date getStartTime() {
            return delegate.getStartTime();
        }

        public Date getEndTime() {
            return delegate.getEndTime();
        }

        public Date getNextFireTime() {
            return delegate.getNextFireTime();
        }

        public Date getPreviousFireTime() {
            return delegate.getPreviousFireTime();
        }

        public Date getFireTimeAfter(Date afterTime) {
            return delegate.getFireTimeAfter(afterTime);
        }

        public Date getFinalFireTime() {
            return delegate.getFinalFireTime();
        }

        public int getMisfireInstruction() {
            return delegate.getMisfireInstruction();
        }

        public TriggerBuilder<? extends Trigger> getTriggerBuilder() {
            return delegate.getTriggerBuilder();
        }

        public ScheduleBuilder<? extends Trigger> getScheduleBuilder() {
            return delegate.getScheduleBuilder();
        }

        public boolean equals(Object other) {
            return delegate.equals(other);
        }

        public int compareTo(Trigger other) {
            return delegate.compareTo(other);
        }

        public String getKeyString() {
            return keyString;
        }

        public OperableTrigger getDelegate() {
            return delegate;
        }

        public void setDelegate(OperableTrigger delegate) {
            this.delegate = delegate;
        }

        public State getState() {
            return state;
        }

        public void setState(State state) {
            this.state = state;
        }

        static enum State {

            WAITING(TriggerState.NORMAL), ACQUIRED(TriggerState.NORMAL), EXECUTING(TriggerState.NORMAL), COMPLETE(TriggerState.COMPLETE),
            BLOCKED(TriggerState.BLOCKED), ERROR(TriggerState.ERROR), PAUSED(TriggerState.PAUSED), PAUSED_BLOCKED(TriggerState.PAUSED),
            DELETED(TriggerState.NORMAL);

            private final TriggerState triggerState;

            State(TriggerState triggerState) {
                this.triggerState = triggerState;
            }

            public TriggerState getTriggerState() {
                return triggerState;
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("state", this.state.toString());
            builder.field("nextFireTime", getNextFireTime());
            builder.field("prevFireTime", getPreviousFireTime());
            builder.field("info", this.stateInfo);
            builder.field("node", "TODO");

            if (delegate instanceof DailyTimeIntervalTrigger) {
                builder.field("timesTriggered", ((DailyTimeIntervalTrigger) delegate).getTimesTriggered());
            } else if (delegate instanceof SimpleTrigger) {
                builder.field("timesTriggered", ((SimpleTrigger) delegate).getTimesTriggered());
            }

            builder.endObject();
            return builder;
        }

        public static StoredOperableTrigger fromAttributeMap(TriggerKey triggerKey, Map<String, Object> attributeMap) {
            StoredOperableTrigger result = new StoredOperableTrigger(triggerKey);

            try {
                result.state = State.valueOf((String) attributeMap.get("state"));
            } catch (Exception e) {
                log.error("Error while parsing trigger " + triggerKey, e);
                result.state = State.ERROR;
                result.stateInfo = "Error while parsing " + e;
            }

            return result;
        }

        public String getStateInfo() {
            return stateInfo;
        }

        public void setStateInfo(String stateInfo) {
            this.stateInfo = stateInfo;
        }

    }

}
