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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.Engine.Delete;
import org.elasticsearch.index.engine.Engine.DeleteResult;
import org.elasticsearch.index.engine.Engine.Index;
import org.elasticsearch.index.engine.Engine.IndexResult;
import org.elasticsearch.index.engine.Engine.Result;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
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
import org.quartz.impl.triggers.CalendarIntervalTriggerImpl;
import org.quartz.impl.triggers.DailyTimeIntervalTriggerImpl;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;

import com.floragunn.searchsupport.jobs.cluster.DistributedJobStore;
import com.floragunn.searchsupport.jobs.config.JobConfig;
import com.floragunn.searchsupport.jobs.config.JobConfigFactory;
import com.floragunn.searchsupport.util.SingleElementBlockingQueue;
import com.google.common.base.Objects;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

public class IndexJobStateStore<JobType extends com.floragunn.searchsupport.jobs.config.JobConfig> implements DistributedJobStore {

    // TODO maybe separate loggers for each scheduler instance?
    private static final Logger log = LogManager.getLogger(IndexJobStateStore.class);

    private final String indexName;
    private final String statusIndexName;
    private final String nodeId;
    private final Client client;
    private SchedulerSignaler signaler;
    private final Map<JobKey, InternalJobDetail> keyToJobMap = new HashMap<>();
    private final Map<TriggerKey, InternalOperableTrigger> keyToTriggerMap = new HashMap<>();
    private final Table<String, JobKey, InternalJobDetail> groupAndKeyToJobMap = HashBasedTable.create();
    private final Table<String, TriggerKey, InternalOperableTrigger> groupAndKeyToTriggerMap = HashBasedTable.create();
    // TODO check for changes that make TreeSet inconsistent
    private final TreeSet<InternalOperableTrigger> activeTriggers = new TreeSet<InternalOperableTrigger>(new Trigger.TriggerTimeComparator());
    private final Set<String> pausedTriggerGroups = new HashSet<String>();
    private final Set<String> pausedJobGroups = new HashSet<String>();
    private final Set<JobKey> blockedJobs = new HashSet<JobKey>();
    private final Iterable<JobType> jobConfigSource;
    private final JobConfigFactory<JobType> jobFactory;
    private boolean schedulerRunning = false;
    private volatile boolean shutdown = false;
    private long misfireThreshold = 5000l;
    private ThreadLocal<Set<InternalOperableTrigger>> dirtyTriggers = ThreadLocal.withInitial(() -> new HashSet<>());
    private final ExecutorService configChangeExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new SingleElementBlockingQueue<Runnable>());

    public IndexJobStateStore(String indexName, String nodeId, Client client, Iterable<JobType> jobConfigSource,
            JobConfigFactory<JobType> jobFactory) {
        this.indexName = indexName;
        this.statusIndexName = indexName + "_status";
        this.nodeId = nodeId;
        this.client = client;
        this.jobConfigSource = jobConfigSource;
        this.jobFactory = jobFactory;
    }

    @Override
    public void clusterConfigChanged() {
        if (shutdown) {
            return;
        }

        // TODO sync with scheduler

        configChangeExecutor.submit(() -> updateAfterClusterConfigChange());
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
        if (!shutdown) {
            log.info("Shutdown of " + this);
            shutdown = true;
            configChangeExecutor.shutdownNow();
        }
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
        InternalOperableTrigger internalOperableTrigger;

        synchronized (this) {
            storeJob(newJob, false);
            internalOperableTrigger = storeTriggerInHeap(newTrigger, false);
        }

        setTriggerStatusInIndex(internalOperableTrigger);
    }

    @Override
    public synchronized void storeJob(JobDetail newJob, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
        InternalJobDetail newJobLocal = new InternalJobDetail(newJob, this);

        addToCollections(newJobLocal);
    }

    @Override
    public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace)
            throws ObjectAlreadyExistsException, JobPersistenceException {

        ArrayList<InternalOperableTrigger> internalOperableTriggers = new ArrayList<>();

        synchronized (this) {
            for (Entry<JobDetail, Set<? extends Trigger>> entry : triggersAndJobs.entrySet()) {
                storeJob(entry.getKey(), true);

                for (Trigger trigger : entry.getValue()) {
                    internalOperableTriggers.add(storeTriggerInHeap((OperableTrigger) trigger, true));
                }
            }
        }

        for (InternalOperableTrigger internalOperableTrigger : internalOperableTriggers) {
            setTriggerStatusInIndex(internalOperableTrigger);
        }
    }

    @Override
    public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
        InternalOperableTrigger internalOperableTrigger = storeTriggerInHeap(newTrigger, replaceExisting);

        setTriggerStatusInIndex(internalOperableTrigger);
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
        InternalOperableTrigger internalOperableTrigger = this.keyToTriggerMap.remove(triggerKey);

        if (internalOperableTrigger == null) {
            return false;
        }

        this.groupAndKeyToTriggerMap.remove(triggerKey.getGroup(), triggerKey);
        this.activeTriggers.remove(internalOperableTrigger);

        InternalJobDetail internalJobDetail = this.keyToJobMap.get(internalOperableTrigger.getJobKey());

        if (internalJobDetail != null) {
            internalJobDetail.triggers.remove(internalOperableTrigger);

            if (internalJobDetail.triggers.isEmpty()) {
                removeJob(internalJobDetail.getKey());
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
        InternalOperableTrigger internalOperableTrigger;

        synchronized (this) {
            internalOperableTrigger = this.keyToTriggerMap.get(triggerKey);

            if (internalOperableTrigger == null) {
                return false;
            }

            internalOperableTrigger.setDelegate(newTrigger);

            if (updateTriggerStateToIdle(internalOperableTrigger)) {
                activeTriggers.add(internalOperableTrigger);
            } else {
                activeTriggers.remove(internalOperableTrigger);
            }
        }

        setTriggerStatusInIndex(internalOperableTrigger);

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
    public Calendar retrieveCalendar(String calName) {
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

            for (Map.Entry<String, Map<JobKey, InternalJobDetail>> entry : this.groupAndKeyToJobMap.rowMap().entrySet()) {
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

            for (Map.Entry<String, Map<TriggerKey, InternalOperableTrigger>> entry : this.groupAndKeyToTriggerMap.rowMap().entrySet()) {
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
        InternalJobDetail internalJobDetail = this.keyToJobMap.get(jobKey);

        if (internalJobDetail == null) {
            return null;
        }

        return internalJobDetail.triggers.stream().map(s -> s.delegate).collect(Collectors.toList());
    }

    @Override
    public synchronized TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
        InternalOperableTrigger internalOperableTrigger = this.keyToTriggerMap.get(triggerKey);

        if (internalOperableTrigger == null) {
            return TriggerState.NONE;
        } else {
            return internalOperableTrigger.state.getTriggerState();
        }
    }

    @Override
    public void resetTriggerFromErrorState(TriggerKey triggerKey) throws JobPersistenceException {
        InternalOperableTrigger internalOperableTrigger;

        synchronized (this) {
            internalOperableTrigger = this.keyToTriggerMap.get(triggerKey);

            if (internalOperableTrigger == null || internalOperableTrigger.state != InternalOperableTrigger.State.ERROR) {
                return;
            }

            if (updateTriggerStateToIdle(internalOperableTrigger)) {
                activeTriggers.add(internalOperableTrigger);
            } else {
                activeTriggers.remove(internalOperableTrigger);
            }
        }

        setTriggerStatusInIndex(internalOperableTrigger);

    }

    @Override
    public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        InternalOperableTrigger internalOperableTrigger;

        synchronized (this) {
            internalOperableTrigger = this.keyToTriggerMap.get(triggerKey);

            if (!pauseTriggerInHeap(internalOperableTrigger)) {
                return;
            }
        }

        setTriggerStatusInIndex(internalOperableTrigger);

    }

    @Override
    public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        Collection<InternalOperableTrigger> matchedTriggers;
        Collection<String> result;

        synchronized (this) {

            matchedTriggers = this.matchTriggers(matcher);

            if (matchedTriggers.isEmpty()) {
                return Collections.emptyList();
            }

            for (InternalOperableTrigger internalOperableTrigger : matchedTriggers) {
                pauseTriggerInHeap(internalOperableTrigger);
            }

            result = matchedTriggers.stream().map(t -> t.getKey().getGroup()).collect(Collectors.toSet());
        }

        flushDirtyTriggersToIndex();

        return result;
    }

    @Override
    public void pauseJob(JobKey jobKey) throws JobPersistenceException {
        synchronized (this) {
            InternalJobDetail internalJobDetail = this.keyToJobMap.get(jobKey);

            if (internalJobDetail == null) {
                return;
            }

            for (InternalOperableTrigger internalOperableTrigger : internalJobDetail.triggers) {
                pauseTriggerInHeap(internalOperableTrigger);
            }
        }

        flushDirtyTriggersToIndex();
    }

    @Override
    public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
        Collection<InternalJobDetail> matchedJobs;
        Collection<String> result;

        synchronized (this) {

            matchedJobs = this.matchJobs(groupMatcher);

            if (matchedJobs.isEmpty()) {
                return Collections.emptyList();
            }

            for (InternalJobDetail internalJobDetail : matchedJobs) {
                for (InternalOperableTrigger internalOperableTrigger : internalJobDetail.triggers) {
                    pauseTriggerInHeap(internalOperableTrigger);
                }
            }

            result = matchedJobs.stream().map(t -> t.getKey().getGroup()).collect(Collectors.toSet());
        }

        flushDirtyTriggersToIndex();

        return result;
    }

    @Override
    public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        InternalOperableTrigger internalOperableTrigger;

        synchronized (this) {
            internalOperableTrigger = this.keyToTriggerMap.get(triggerKey);

            if (!resumeTriggerInHeap(internalOperableTrigger)) {
                return;
            }

        }

        setTriggerStatusInIndex(internalOperableTrigger);
    }

    @Override
    public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        Collection<InternalOperableTrigger> matchedTriggers;
        Collection<String> result;

        synchronized (this) {

            matchedTriggers = this.matchTriggers(matcher);

            if (matchedTriggers.isEmpty()) {
                return Collections.emptyList();
            }

            for (InternalOperableTrigger internalOperableTrigger : matchedTriggers) {
                resumeTriggerInHeap(internalOperableTrigger);
            }

            result = matchedTriggers.stream().map(t -> t.getKey().getGroup()).collect(Collectors.toSet());
        }

        flushDirtyTriggersToIndex();

        return result;
    }

    @Override
    public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void resumeJob(JobKey jobKey) throws JobPersistenceException {
        synchronized (this) {
            InternalJobDetail internalJobDetail = this.keyToJobMap.get(jobKey);

            if (internalJobDetail == null) {
                return;
            }

            for (InternalOperableTrigger internalOperableTrigger : internalJobDetail.triggers) {
                resumeTriggerInHeap(internalOperableTrigger);
            }
        }

        flushDirtyTriggersToIndex();
    }

    @Override
    public Collection<String> resumeJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
        Collection<InternalJobDetail> matchedJobs;
        Collection<String> result;

        synchronized (this) {

            matchedJobs = this.matchJobs(groupMatcher);

            if (matchedJobs.isEmpty()) {
                return Collections.emptyList();
            }

            for (InternalJobDetail internalJobDetail : matchedJobs) {
                for (InternalOperableTrigger internalOperableTrigger : internalJobDetail.triggers) {
                    resumeTriggerInHeap(internalOperableTrigger);
                }
            }

            result = matchedJobs.stream().map(t -> t.getKey().getGroup()).collect(Collectors.toSet());
        }

        flushDirtyTriggersToIndex();

        return result;
    }

    @Override
    public void pauseAll() throws JobPersistenceException {
        synchronized (this) {
            for (InternalOperableTrigger internalOperableTrigger : this.keyToTriggerMap.values()) {
                pauseTriggerInHeap(internalOperableTrigger);
            }
        }

        flushDirtyTriggersToIndex();
    }

    @Override
    public void resumeAll() throws JobPersistenceException {

        synchronized (this) {
            for (InternalOperableTrigger internalOperableTrigger : this.keyToTriggerMap.values()) {
                resumeTriggerInHeap(internalOperableTrigger);
            }
        }

        flushDirtyTriggersToIndex();
    }

    @Override
    public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow) throws JobPersistenceException {
        if (log.isDebugEnabled()) {
            log.debug("acquireNextTriggers(noLaterThan = " + new Date(noLaterThan) + ", maxCount = " + maxCount + ", timeWindow =" + timeWindow
                    + ") for " + this);
        }

        List<OperableTrigger> result;

        synchronized (this) {
            if (log.isDebugEnabled()) {
                log.debug("Number of active triggers: " + this.activeTriggers.size());
            }

            if (this.activeTriggers.isEmpty()) {
                return Collections.emptyList();
            }

            log.debug("Active triggers: " + activeTriggers);

            result = new ArrayList<OperableTrigger>(Math.min(maxCount, this.activeTriggers.size()));
            Set<JobKey> acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();
            Set<InternalOperableTrigger> excludedTriggers = new HashSet<InternalOperableTrigger>();
            long misfireIsBefore = System.currentTimeMillis() - misfireThreshold;

            long batchEnd = noLaterThan;

            for (InternalOperableTrigger trigger = activeTriggers.pollFirst(); trigger != null
                    && result.size() < maxCount; trigger = activeTriggers.pollFirst()) {
                if (trigger.getNextFireTime() == null) {
                    continue;
                }

                if (checkForMisfire(trigger, misfireIsBefore)) {
                    if (trigger.getNextFireTime() != null) {
                        this.activeTriggers.add(trigger);
                    }
                    markDirty(trigger);
                    continue;
                }

                if (trigger.getNextFireTime().getTime() > batchEnd) {
                    activeTriggers.add(trigger);
                    break;
                }

                InternalJobDetail internalJobDetail = keyToJobMap.get(trigger.getJobKey());

                if (internalJobDetail.isConcurrentExectionDisallowed()) {
                    if (acquiredJobKeysForNoConcurrentExec.contains(internalJobDetail.getKey())) {
                        excludedTriggers.add(trigger);
                        continue;
                    } else {
                        acquiredJobKeysForNoConcurrentExec.add(trigger.getJobKey());
                    }
                }

                trigger.setStateAndNode(InternalOperableTrigger.State.ACQUIRED, nodeId);

                // tw.trigger.setFireInstanceId(getFiredTriggerRecordId()); TODO

                if (result.isEmpty()) {
                    batchEnd = Math.max(trigger.getNextFireTime().getTime(), System.currentTimeMillis()) + timeWindow;
                }

                result.add(trigger);
            }

            this.activeTriggers.addAll(excludedTriggers);
        }

        flushDirtyTriggersToIndex();

        log.debug("Result: " + result);

        return result;
    }

    @Override
    public void releaseAcquiredTrigger(OperableTrigger trigger) {
        InternalOperableTrigger internalOperableTrigger;

        synchronized (this) {
            internalOperableTrigger = this.keyToTriggerMap.get(trigger.getKey());

            if (internalOperableTrigger == null) {
                return;
            }

            if (internalOperableTrigger.state != InternalOperableTrigger.State.ACQUIRED) {
                return;
            }

            internalOperableTrigger.state = InternalOperableTrigger.State.WAITING;

            activeTriggers.add(internalOperableTrigger);

        }

        try {

            setTriggerStatusInIndex(internalOperableTrigger);
        } catch (Exception e) {
            // TODO
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> firedTriggers) throws JobPersistenceException {
        List<TriggerFiredResult> results = new ArrayList<TriggerFiredResult>(firedTriggers.size());

        if (log.isDebugEnabled()) {
            log.debug("triggersFired(" + firedTriggers + ")");
        }

        synchronized (this) {

            for (OperableTrigger trigger : firedTriggers) {
                InternalOperableTrigger internalOperableTrigger = toInternal(trigger);

                if (internalOperableTrigger == null) {
                    continue;
                }

                // was the trigger completed, paused, blocked, etc. since being acquired?
                if (internalOperableTrigger.state != InternalOperableTrigger.State.ACQUIRED) {
                    continue;
                }

                Date prevFireTime = trigger.getPreviousFireTime();

                activeTriggers.remove(internalOperableTrigger);
                internalOperableTrigger.triggered(null);
                internalOperableTrigger.state = InternalOperableTrigger.State.EXECUTING;
                internalOperableTrigger.node = nodeId;
                markDirty(internalOperableTrigger);

                InternalJobDetail jobDetail = this.keyToJobMap.get(trigger.getJobKey());

                TriggerFiredBundle triggerFiredBundle = new TriggerFiredBundle(jobDetail, trigger, null, false, new Date(),
                        trigger.getPreviousFireTime(), prevFireTime, trigger.getNextFireTime());

                if (jobDetail.isConcurrentExectionDisallowed()) {
                    jobDetail.blockIdleTriggers();
                } else if (internalOperableTrigger.getNextFireTime() != null) {
                    this.activeTriggers.add(internalOperableTrigger);
                }

                results.add(new TriggerFiredResult(triggerFiredBundle));
            }
        }

        flushDirtyTriggersToIndex();

        if (log.isDebugEnabled()) {
            log.debug("triggersFired() = " + results);

        }

        return results;

    }

    @Override
    public void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail, CompletedExecutionInstruction triggerInstCode) {
        if (log.isDebugEnabled()) {
            log.debug("triggeredJobComplete(" + trigger + ")");
        }

        synchronized (this) {
            InternalJobDetail internalJobDetail = toInternal(jobDetail);
            InternalOperableTrigger internalOperableTrigger = toInternal(trigger);

            if (internalJobDetail != null) {
                if (internalJobDetail.isPersistJobDataAfterExecution()) {
                    JobDataMap newData = jobDetail.getJobDataMap();
                    if (newData != null) {
                        newData = (JobDataMap) newData.clone();
                        newData.clearDirtyFlag();
                    }
                    // TODO persist?!
                    internalJobDetail.delegate = internalJobDetail.getJobBuilder().setJobData(newData).build();
                }

                if (internalJobDetail.isConcurrentExectionDisallowed()) {
                    internalJobDetail.deblockTriggers();
                    signaler.signalSchedulingChange(0L);
                }
            } else {
                blockedJobs.remove(jobDetail.getKey());
            }

            if (internalOperableTrigger != null) {
                switch (triggerInstCode) {
                case DELETE_TRIGGER:
                    // TODO sinnvoll bei extern geladener config?
                    internalOperableTrigger.setState(InternalOperableTrigger.State.DELETED);

                    if (trigger.getNextFireTime() == null) {
                        // double check for possible reschedule within job 
                        // execution, which would cancel the need to delete...
                        if (internalOperableTrigger.getNextFireTime() == null) {
                            removeTrigger(internalOperableTrigger.getKey());
                        }
                    } else {
                        removeTrigger(trigger.getKey());
                        signaler.signalSchedulingChange(0L);
                    }
                    break;
                case SET_TRIGGER_COMPLETE:
                    internalOperableTrigger.setState(InternalOperableTrigger.State.COMPLETE);
                    this.activeTriggers.remove(internalOperableTrigger);
                    signaler.signalSchedulingChange(0L);
                    break;
                case SET_TRIGGER_ERROR:
                    internalOperableTrigger.setState(InternalOperableTrigger.State.ERROR);
                    this.activeTriggers.remove(internalOperableTrigger);
                    signaler.signalSchedulingChange(0L);
                    break;
                case SET_ALL_JOB_TRIGGERS_ERROR:
                    setAllTriggersOfJobToState(internalJobDetail, InternalOperableTrigger.State.ERROR);
                    signaler.signalSchedulingChange(0L);
                    break;
                case SET_ALL_JOB_TRIGGERS_COMPLETE:
                    setAllTriggersOfJobToState(internalJobDetail, InternalOperableTrigger.State.COMPLETE);
                    signaler.signalSchedulingChange(0L);
                    break;
                default:
                    internalOperableTrigger.setState(InternalOperableTrigger.State.WAITING);
                    this.activeTriggers.add(internalOperableTrigger);
                    break;
                }
            }
        }

        flushDirtyTriggersToIndex();

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

    @SuppressWarnings("unchecked")
    private InternalJobDetail toInternal(JobDetail jobDetail) {
        if (jobDetail instanceof IndexJobStateStore.InternalJobDetail) {
            return (InternalJobDetail) jobDetail;
        } else {
            return this.keyToJobMap.get(jobDetail.getKey());
        }
    }

    private InternalOperableTrigger toInternal(OperableTrigger trigger) {
        if (trigger instanceof InternalOperableTrigger) {
            return (InternalOperableTrigger) trigger;
        } else {
            return this.keyToTriggerMap.get(trigger.getKey());
        }
    }

    private synchronized InternalOperableTrigger storeTriggerInHeap(OperableTrigger newTrigger, boolean replaceExisting)
            throws ObjectAlreadyExistsException, JobPersistenceException {

        InternalJobDetail internalJobDetail = this.keyToJobMap.get(newTrigger.getJobKey());

        if (internalJobDetail == null) {
            throw new JobPersistenceException("Trigger " + newTrigger + " references non-existing job" + newTrigger.getJobKey());
        }

        InternalOperableTrigger internalOperableTrigger = new InternalOperableTrigger(newTrigger);

        internalJobDetail.addTrigger(internalOperableTrigger);

        addToCollections(internalOperableTrigger);

        updateTriggerStateToIdle(internalOperableTrigger);

        return internalOperableTrigger;
    }

    private synchronized void addToCollections(InternalJobDetail internalJobDetail) {
        keyToJobMap.put(internalJobDetail.getKey(), internalJobDetail);
        groupAndKeyToJobMap.put(internalJobDetail.getKey().getGroup(), internalJobDetail.getKey(), internalJobDetail);

        if (!internalJobDetail.triggers.isEmpty()) {
            for (InternalOperableTrigger internalOperableTrigger : internalJobDetail.triggers) {
                this.addToCollections(internalOperableTrigger);
            }
        }
    }

    private synchronized void addToCollections(InternalOperableTrigger internalOperableTrigger) {
        this.groupAndKeyToTriggerMap.put(internalOperableTrigger.getKey().getGroup(), internalOperableTrigger.getKey(), internalOperableTrigger);
        this.keyToTriggerMap.put(internalOperableTrigger.getKey(), internalOperableTrigger);
    }

    private synchronized void initActiveTriggers() {
        activeTriggers.clear();

        for (InternalOperableTrigger trigger : this.keyToTriggerMap.values()) {
            if (trigger.state == InternalOperableTrigger.State.WAITING) {
                activeTriggers.add(trigger);
            }
        }
    }

    private boolean updateTriggerStateToIdle(InternalOperableTrigger internalOperableTrigger) {
        if (pausedTriggerGroups.contains(internalOperableTrigger.getKey().getGroup())
                || pausedJobGroups.contains(internalOperableTrigger.getJobKey().getGroup())) {
            if (blockedJobs.contains(internalOperableTrigger.getJobKey())) {
                internalOperableTrigger.setState(InternalOperableTrigger.State.PAUSED_BLOCKED);
            } else {
                internalOperableTrigger.setState(InternalOperableTrigger.State.PAUSED);
            }
            return false;
        } else if (blockedJobs.contains(internalOperableTrigger.getJobKey())) {
            internalOperableTrigger.setState(InternalOperableTrigger.State.BLOCKED);
            return false;
        } else {
            internalOperableTrigger.setState(InternalOperableTrigger.State.WAITING);
            return true;
        }
    }

    private void setTriggerStatusInIndex(InternalOperableTrigger internalOperableTrigger) throws JobPersistenceException {
        try {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
            internalOperableTrigger.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);

            IndexRequest indexRequest = new IndexRequest(statusIndexName).id(internalOperableTrigger.getKeyString()).source(xContentBuilder);

            // TODO how to handle failure
            this.client.index(indexRequest);
        } catch (Exception e) {
            throw new JobPersistenceException(e.getMessage(), e);
        }
    }

    private synchronized Collection<InternalOperableTrigger> matchTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        if (matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            return this.groupAndKeyToTriggerMap.row(matcher.getCompareToValue()).values();
        } else {
            HashSet<InternalOperableTrigger> result = new HashSet<>();
            StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
            String matcherValue = matcher.getCompareToValue();

            for (Map.Entry<String, Map<TriggerKey, InternalOperableTrigger>> entry : this.groupAndKeyToTriggerMap.rowMap().entrySet()) {
                if (operator.evaluate(entry.getKey(), matcherValue)) {
                    result.addAll(entry.getValue().values());
                }
            }

            return result;
        }
    }

    private synchronized Collection<InternalJobDetail> matchJobs(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        if (matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            return this.groupAndKeyToJobMap.row(matcher.getCompareToValue()).values();
        } else {
            HashSet<InternalJobDetail> result = new HashSet<>();
            StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
            String matcherValue = matcher.getCompareToValue();

            for (Map.Entry<String, Map<JobKey, InternalJobDetail>> entry : this.groupAndKeyToJobMap.rowMap().entrySet()) {
                if (operator.evaluate(entry.getKey(), matcherValue)) {
                    result.addAll(entry.getValue().values());
                }
            }

            return result;
        }
    }

    private synchronized boolean pauseTriggerInHeap(InternalOperableTrigger internalOperableTrigger) {
        if (internalOperableTrigger == null || internalOperableTrigger.state == InternalOperableTrigger.State.COMPLETE) {
            return false;
        }

        if (internalOperableTrigger.state == InternalOperableTrigger.State.BLOCKED) {
            internalOperableTrigger.state = InternalOperableTrigger.State.PAUSED_BLOCKED;
        } else {
            internalOperableTrigger.state = InternalOperableTrigger.State.PAUSED;
        }

        activeTriggers.remove(internalOperableTrigger);
        markDirty(internalOperableTrigger);

        return true;
    }

    private synchronized boolean resumeTriggerInHeap(InternalOperableTrigger internalOperableTrigger) {
        if (internalOperableTrigger == null) {
            return false;
        }

        if (internalOperableTrigger.state != InternalOperableTrigger.State.PAUSED
                && internalOperableTrigger.state != InternalOperableTrigger.State.PAUSED_BLOCKED) {
            return false;
        }

        internalOperableTrigger.state = InternalOperableTrigger.State.WAITING;

        checkForMisfire(internalOperableTrigger);

        if (updateTriggerStateToIdle(internalOperableTrigger)) {
            activeTriggers.add(internalOperableTrigger);
        } else {
            activeTriggers.remove(internalOperableTrigger);
        }

        markDirty(internalOperableTrigger);
        return true;
    }

    private synchronized void setAllTriggersOfJobToState(InternalJobDetail internalJobDetail, InternalOperableTrigger.State state) {

        for (InternalOperableTrigger internalOperableTrigger : internalJobDetail.triggers) {
            if (internalOperableTrigger.state == state) {
                continue;
            }

            internalOperableTrigger.state = state;

            if (state != InternalOperableTrigger.State.WAITING) {
                this.activeTriggers.remove(internalOperableTrigger);
            }

            markDirty(internalOperableTrigger);
        }

    }

    private void initJobs() {
        Collection<InternalJobDetail> jobs = this.loadJobs();

        synchronized (this) {
            resetJobs();

            for (InternalJobDetail job : jobs) {
                addToCollections(job);
            }

            initActiveTriggers();
        }

        flushDirtyTriggersToIndex();
    }

    private void updateAfterClusterConfigChange() {
        try {
            log.info("Reinitializing jobs for " + IndexJobStateStore.this);
            initJobs();
            signaler.signalSchedulingChange(0L);
        } catch (Exception e) {
            try {
                // Let a potential cluster shutdown catch up
                Thread.sleep(500);
            } catch (InterruptedException e1) {
                log.debug(e1);
            }
            if (!shutdown) {
                log.error("Error while initializing jobs for " + IndexJobStateStore.this, e);
                // TODO retry?
            }
        }
    }

    private synchronized void resetJobs() {
        keyToJobMap.clear();
        keyToTriggerMap.clear();
        groupAndKeyToJobMap.clear();
        groupAndKeyToTriggerMap.clear();
        activeTriggers.clear();
    }

    private Collection<InternalJobDetail> loadJobs() {
        Set<JobType> jobConfigSet = this.loadJobConfig();

        if (log.isDebugEnabled()) {
            log.debug("Job configurations loaded: " + jobConfigSet);
        }

        Map<TriggerKey, InternalOperableTrigger> triggerStates = this.loadTriggerStates(jobConfigSet);
        Collection<InternalJobDetail> result = new ArrayList<>(jobConfigSet.size());

        for (JobType jobConfig : jobConfigSet) {
            InternalJobDetail internalJobDetail = new InternalJobDetail(this.jobFactory.createJobDetail(jobConfig), this);

            for (Trigger triggerConfig : jobConfig.getTriggers()) {
                if (!(triggerConfig instanceof OperableTrigger)) {
                    log.error("Trigger is not OperableTrigger: " + triggerConfig);
                    continue;
                }

                OperableTrigger operableTriggerConfig = (OperableTrigger) triggerConfig;
                InternalOperableTrigger internalOperableTrigger = triggerStates.get(triggerConfig.getKey());

                if (internalOperableTrigger != null) {
                    internalOperableTrigger.setDelegate(operableTriggerConfig);
                    checkTriggerStateAfterRecovery(internalOperableTrigger);
                } else {
                    internalOperableTrigger = new InternalOperableTrigger(operableTriggerConfig);
                    internalOperableTrigger.computeFirstFireTime(null);
                    internalOperableTrigger.node = this.nodeId;

                    updateTriggerStateToIdle(internalOperableTrigger);
                }

                internalJobDetail.addTrigger(internalOperableTrigger);
            }

            result.add(internalJobDetail);
        }

        if (log.isInfoEnabled()) {
            log.info("Jobs loaded: " + result);
        }

        return result;
    }

    private synchronized void checkTriggerStateAfterRecovery(InternalOperableTrigger internalOperableTrigger) {
        switch (internalOperableTrigger.getState()) {
        case EXECUTING:
            if (this.nodeId.equals(internalOperableTrigger.getNode())) {
                log.info("Trigger " + internalOperableTrigger + " is still executing on local node.");
            } else {
                log.info("Trigger " + internalOperableTrigger + " is marked as still executing on node " + internalOperableTrigger.getNode());
                // TODO What to do? Ask the other node if it is still executing? Timeout?
            }
            break;
        case ACQUIRED:
        case BLOCKED:
        case WAITING:
            if (internalOperableTrigger.getNextFireTime() == null) {
                internalOperableTrigger.computeFirstFireTime(null);
            }
            internalOperableTrigger.node = this.nodeId;
            updateTriggerStateToIdle(internalOperableTrigger);
            break;
        case PAUSED_BLOCKED:
            if (internalOperableTrigger.getNextFireTime() == null) {
                internalOperableTrigger.computeFirstFireTime(null);
            }
            internalOperableTrigger.setStateAndNode(InternalOperableTrigger.State.PAUSED, this.nodeId);
            break;
        default:
            // No change needed
            break;
        }
    }

    private Map<TriggerKey, InternalOperableTrigger> loadTriggerStates(Set<JobType> jobConfig) {
        try {
            Map<String, TriggerKey> triggerIds = this.getTriggerIds(jobConfig);

            if (triggerIds.isEmpty()) {
                return Collections.emptyMap();
            }

            Map<TriggerKey, InternalOperableTrigger> result = new HashMap<>(triggerIds.size());

            QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds(triggerIds.keySet().toArray(new String[triggerIds.size()]));

            SearchResponse searchResponse = client.prepareSearch(this.statusIndexName).setQuery(queryBuilder).get();

            for (SearchHit searchHit : searchResponse.getHits().getHits()) {
                try {
                    TriggerKey triggerKey = triggerIds.get(searchHit.getId());

                    InternalOperableTrigger internalOperableTrigger = InternalOperableTrigger.fromAttributeMap(triggerKey,
                            searchHit.getSourceAsMap());

                    result.put(triggerKey, internalOperableTrigger);

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

    private boolean checkForMisfire(InternalOperableTrigger internalOperableTrigger) {
        return checkForMisfire(internalOperableTrigger, System.currentTimeMillis() - misfireThreshold);
    }

    private boolean checkForMisfire(InternalOperableTrigger internalOperableTrigger, long isMisfireBefore) {

        Date nextFireTime = internalOperableTrigger.getNextFireTime();

        if (nextFireTime == null || nextFireTime.getTime() > isMisfireBefore
                || internalOperableTrigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) {
            return false;
        }

        Calendar calendar = null;
        if (internalOperableTrigger.getCalendarName() != null) {
            calendar = retrieveCalendar(internalOperableTrigger.getCalendarName());
        }

        signaler.notifyTriggerListenersMisfired((OperableTrigger) internalOperableTrigger.getDelegate().clone());

        internalOperableTrigger.updateAfterMisfire(calendar);

        this.markDirty(internalOperableTrigger);

        if (internalOperableTrigger.getNextFireTime() == null) {
            synchronized (this) {
                internalOperableTrigger.state = InternalOperableTrigger.State.COMPLETE;
                this.activeTriggers.remove(internalOperableTrigger);
            }
            signaler.notifySchedulerListenersFinalized(internalOperableTrigger);
            return true;

        } else if (nextFireTime.equals(internalOperableTrigger.getNextFireTime())) {
            return false;
        } else {
            return true;
        }
    }

    private void markDirty(InternalOperableTrigger trigger) {
        this.dirtyTriggers.get().add(trigger);
    }

    private void flushDirtyTriggersToIndex() {
        for (OperableTrigger trigger : this.dirtyTriggers.get()) {
            // TODO batch
            try {
                setTriggerStatusInIndex((InternalOperableTrigger) trigger);
            } catch (Exception e) {
                log.error("error while flushing triggers", e);
            }
        }

        this.dirtyTriggers.get().clear();
    }

    static class InternalJobDetail implements JobDetail {

        private static final long serialVersionUID = -4500332272991179774L;

        private JobDetail delegate;
        private final IndexJobStateStore<?> jobStore;
        private List<InternalOperableTrigger> triggers = new ArrayList<>();

        InternalJobDetail(JobDetail jobDetail, IndexJobStateStore<?> jobStore) {
            this.delegate = jobDetail;
            this.jobStore = jobStore;
        }

        public void addTrigger(InternalOperableTrigger trigger) {
            this.triggers.add(trigger);
            trigger.setJobDetail(this);
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
            return new InternalJobDetail(this.delegate, this.jobStore);
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
            return "InternalJobDetail [key=" + getKey() + ", class=" + getJobClass() + ", jobDataMap=" + new HashMap<>(getJobDataMap())
                    + ", triggers=" + triggers + "]";
        }

        void blockIdleTriggers() {
            for (InternalOperableTrigger trigger : triggers) {
                if (trigger.state == InternalOperableTrigger.State.WAITING) {
                    trigger.setState(InternalOperableTrigger.State.BLOCKED);
                } else if (trigger.state == InternalOperableTrigger.State.PAUSED) {
                    trigger.setState(InternalOperableTrigger.State.PAUSED_BLOCKED);
                }

                this.jobStore.activeTriggers.remove(trigger);
            }

            this.jobStore.blockedJobs.add(getKey());
        }

        void deblockTriggers() {
            this.jobStore.blockedJobs.remove(getKey());

            for (InternalOperableTrigger trigger : triggers) {
                if (trigger.state == InternalOperableTrigger.State.BLOCKED) {
                    trigger.setState(InternalOperableTrigger.State.WAITING);
                    this.jobStore.activeTriggers.add(trigger);
                } else if (trigger.state == InternalOperableTrigger.State.PAUSED_BLOCKED) {
                    trigger.setState(InternalOperableTrigger.State.PAUSED);
                }
            }
        }
    }

    static class InternalOperableTrigger implements OperableTrigger, ToXContentObject {
        private static final long serialVersionUID = -181071146931763579L;
        private OperableTrigger delegate;
        private final TriggerKey key;
        private final String keyString;
        private State state = State.WAITING;
        private String stateInfo = null;
        private String node;
        private Date previousFireTime;
        private Date nextFireTime;
        private Integer timesTriggered;
        private InternalJobDetail jobDetail;

        InternalOperableTrigger(TriggerKey key) {
            this.key = key;
            this.keyString = quartzKeyToKeyString(key);
        }

        InternalOperableTrigger(OperableTrigger operableTrigger) {
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
            return new InternalOperableTrigger(delegate);
        }

        public void setFireInstanceId(String id) {
            delegate.setFireInstanceId(id);
        }

        public String getFireInstanceId() {
            return delegate.getFireInstanceId();
        }

        public void setNextFireTime(Date nextFireTime) {
            this.nextFireTime = nextFireTime;

            if (delegate != null) {
                delegate.setNextFireTime(nextFireTime);
            }
        }

        public void setPreviousFireTime(Date previousFireTime) {
            this.previousFireTime = previousFireTime;

            if (delegate != null) {
                delegate.setPreviousFireTime(previousFireTime);
            }
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
            if (delegate != null) {
                return delegate.getNextFireTime();
            } else {
                return nextFireTime;
            }
        }

        public Date getPreviousFireTime() {
            if (delegate != null) {
                return delegate.getPreviousFireTime();
            } else {
                return previousFireTime;
            }
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

            if (delegate != null) {
                delegate.setPreviousFireTime(this.previousFireTime);
                delegate.setNextFireTime(this.nextFireTime);

                this.setTimesTriggeredInDelegate(this.delegate, this.timesTriggered);
            }
        }

        public State getState() {
            return state;
        }

        public void setState(State state) {
            if (this.state == state) {
                return;
            }

            this.state = state;
            markDirty();
        }

        public void setStateAndNode(State state, String nodeId) {
            if (this.state == state && Objects.equal(this.node, nodeId)) {
                return;
            }

            this.state = state;
            this.node = nodeId;
            markDirty();
        }

        public String toString() {
            return key + " " + state + " " + this.getPreviousFireTime() + " <-> " + this.getNextFireTime();
        }

        void markDirty() {
            if (this.jobDetail != null) {
                this.jobDetail.jobStore.markDirty(this);
            }
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
            builder.field("nextFireTime", getNextFireTime() != null ? getNextFireTime().getTime() : null);
            builder.field("prevFireTime", getPreviousFireTime() != null ? getPreviousFireTime().getTime() : null);
            builder.field("info", this.stateInfo);
            builder.field("node", this.node);
            builder.field("timesTriggered", this.getTimesTriggered());
            builder.endObject();
            return builder;
        }

        public static InternalOperableTrigger fromAttributeMap(TriggerKey triggerKey, Map<String, Object> attributeMap) {
            InternalOperableTrigger result = new InternalOperableTrigger(triggerKey);

            try {
                result.state = State.valueOf((String) attributeMap.get("state"));
                result.node = (String) attributeMap.get("node");
                result.setNextFireTime(toDate(attributeMap.get("nextFireTime")));
                result.setPreviousFireTime(toDate(attributeMap.get("prevFireTime")));
                result.stateInfo = (String) attributeMap.get("info");
                result.setTimesTriggered(
                        attributeMap.get("timesTriggered") instanceof Number ? ((Number) attributeMap.get("timesTriggered")).intValue() : null);

            } catch (Exception e) {
                log.error("Error while parsing trigger " + triggerKey, e);
                result.state = State.ERROR;
                result.stateInfo = "Error while parsing " + e;
            }

            return result;
        }

        private static Date toDate(Object time) {
            if (time instanceof Number) {
                return new Date(((Number) time).longValue());
            } else {
                return null;
            }
        }

        public String getStateInfo() {
            return stateInfo;
        }

        public void setStateInfo(String stateInfo) {
            this.stateInfo = stateInfo;
        }

        public String getNode() {
            return node;
        }

        public void setNode(String node) {
            this.node = node;
        }

        public Integer getTimesTriggered() {
            if (delegate instanceof DailyTimeIntervalTrigger) {
                return ((DailyTimeIntervalTrigger) delegate).getTimesTriggered();
            } else if (delegate instanceof SimpleTrigger) {
                return ((SimpleTrigger) delegate).getTimesTriggered();
            } else {
                return this.timesTriggered;
            }
        }

        public void setTimesTriggered(Integer timesTriggered) {
            this.timesTriggered = timesTriggered;

            setTimesTriggeredInDelegate(delegate, timesTriggered);
        }

        public void setTimesTriggeredInDelegate(OperableTrigger delegate, Integer timesTriggered) {
            if (delegate instanceof CalendarIntervalTriggerImpl) {
                ((CalendarIntervalTriggerImpl) delegate).setTimesTriggered(timesTriggered != null ? timesTriggered : 0);
            } else if (delegate instanceof DailyTimeIntervalTriggerImpl) {
                ((DailyTimeIntervalTriggerImpl) delegate).setTimesTriggered(timesTriggered != null ? timesTriggered : 0);
            } else if (delegate instanceof SimpleTriggerImpl) {
                ((SimpleTriggerImpl) delegate).setTimesTriggered(timesTriggered != null ? timesTriggered : 0);
            }
        }

        public InternalJobDetail getJobDetail() {
            return jobDetail;
        }

        public void setJobDetail(InternalJobDetail jobDetail) {
            this.jobDetail = jobDetail;
        }

    }

    class UpdateListener implements IndexingOperationListener {

        @Override
        public void postIndex(ShardId shardId, Index index, IndexResult result) {
            if (!isForConfiguredIndex(shardId)) {
                return;
            }

            if (result.getResultType() != Result.Type.SUCCESS) {
                return;
            }
        }

        @Override
        public void postDelete(ShardId shardId, Delete delete, DeleteResult result) {
            if (!isForConfiguredIndex(shardId)) {
                return;
            }

            if (result.getResultType() != Result.Type.SUCCESS) {
                return;
            }

        }

        private boolean isForConfiguredIndex(ShardId shardId) {
            return IndexJobStateStore.this.indexName.equals(shardId.getIndexName());
        }
    }
}
