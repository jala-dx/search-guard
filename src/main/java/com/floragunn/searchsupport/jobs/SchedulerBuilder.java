package com.floragunn.searchsupport.jobs;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.quartz.Calendar;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.ListenerManager;
import org.quartz.Scheduler;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;
import org.quartz.SchedulerMetaData;
import org.quartz.Trigger;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.UnableToInterruptJobException;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobFactory;
import org.quartz.spi.JobStore;
import org.quartz.spi.SchedulerPlugin;
import org.quartz.spi.ThreadPool;

import com.carrotsearch.hppc.ObjectHashSet;
import com.floragunn.searchsupport.jobs.cluster.DistributedJobStore;
import com.floragunn.searchsupport.jobs.cluster.JobDistributor;
import com.floragunn.searchsupport.jobs.cluster.NodeComparator;
import com.floragunn.searchsupport.jobs.cluster.NodeIdComparator;
import com.floragunn.searchsupport.jobs.config.IndexJobConfigSource;
import com.floragunn.searchsupport.jobs.config.JobConfig;
import com.floragunn.searchsupport.jobs.config.JobConfigFactory;
import com.floragunn.searchsupport.jobs.core.IndexJobStateStore;

import joptsimple.internal.Strings;

public class SchedulerBuilder<JobType extends JobConfig> {
    private final static Logger log = LogManager.getLogger(SchedulerBuilder.class);

    private String name;
    private String configIndex;
    private String stateIndex;
    private Client client;
    private int maxThreads = 3;
    private int threadPriority = Thread.NORM_PRIORITY;
    private JobConfigFactory<JobType> jobConfigFactory;
    private Iterable<JobType> jobConfigSource;
    private JobStore jobStore;
    private JobDistributor jobDistributor;
    private ThreadPool threadPool;
    private String nodeFilter;
    private ClusterService clusterService;
    private Map<String, SchedulerPlugin> schedulerPluginMap = new HashMap<>();
    private NodeComparator<?> nodeComparator;
    private String nodeId;
    private JobFactory jobFactory;
    private Settings nodeSettings;
    private NodeEnvironment nodeEnvironment;

    public SchedulerBuilder<JobType> name(String name) {
        this.name = name;
        return this;
    }

    public SchedulerBuilder<JobType> configIndex(String configIndex) {
        this.configIndex = configIndex;
        return this;
    }

    public SchedulerBuilder<JobType> stateIndex(String stateIndex) {
        this.stateIndex = stateIndex;
        return this;
    }

    public SchedulerBuilder<JobType> distributed(ClusterService clusterService) {
        this.clusterService = clusterService;
        return this;
    }

    public SchedulerBuilder<JobType> nodeFilter(String nodeFilter) {
        this.nodeFilter = nodeFilter;
        return this;
    }

    public SchedulerBuilder<JobType> client(Client client) {
        this.client = client;
        return this;
    }

    public SchedulerBuilder<JobType> maxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
        return this;
    }

    public SchedulerBuilder<JobType> threadPriority(int threadPriority) {
        this.threadPriority = threadPriority;
        return this;
    }

    public SchedulerBuilder<JobType> jobConfigFactory(JobConfigFactory<JobType> jobFactory) {
        this.jobConfigFactory = jobFactory;
        return this;
    }

    public SchedulerBuilder<JobType> jobFactory(JobFactory jobFactory) {
        this.jobFactory = jobFactory;
        return this;
    }

    public SchedulerBuilder<JobType> jobConfigSource(Iterable<JobType> jobConfigSource) {
        this.jobConfigSource = jobConfigSource;
        return this;
    }

    public SchedulerBuilder<JobType> jobStore(JobStore jobStore) {
        this.jobStore = jobStore;
        return this;
    }

    public SchedulerBuilder<JobType> threadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
        return this;
    }

    public SchedulerBuilder<JobType> nodeComparator(NodeComparator<?> nodeComparator) {
        this.nodeComparator = nodeComparator;
        return this;
    }

    public SchedulerBuilder<JobType> nodeSettings(Settings nodeSettings) {
        this.nodeSettings = nodeSettings;
        return this;
    }

    public SchedulerBuilder<JobType> nodeEnvironment(NodeEnvironment nodeEnvironment) {
        this.nodeEnvironment = nodeEnvironment;
        return this;
    }

    public Scheduler build() throws SchedulerException {
        if (isSchedulerPermanentlyDisabledForLocalNode()) {
            log.info("Scheduler " + name + "is disabled for this node by node filter: " + this.nodeFilter);
            return new DisabledScheduler(name);
        }

        if (this.configIndex == null) {
            this.configIndex = name;
        }

        if (this.stateIndex == null) {
            this.stateIndex = this.configIndex + "_state";
        }

        if (this.nodeComparator == null && clusterService != null) {
            this.nodeComparator = new NodeIdComparator(clusterService);
        }

        if (this.jobDistributor == null && clusterService != null) {

            this.jobDistributor = new JobDistributor(name, nodeFilter, clusterService, null, this.nodeComparator);
        }

        if (this.jobConfigSource == null) {
            this.jobConfigSource = new IndexJobConfigSource<>(configIndex, client, jobConfigFactory, jobDistributor);
        }

        if (this.nodeEnvironment != null) {
            this.nodeId = this.nodeEnvironment.nodeId();
        }

        if (this.jobStore == null) {
            this.jobStore = new IndexJobStateStore<>(name, stateIndex, nodeId, client, jobConfigSource, jobConfigFactory, clusterService);
        }

        if (this.jobStore instanceof DistributedJobStore && this.jobDistributor != null) {
            this.jobDistributor.setDistributedJobStore((DistributedJobStore) this.jobStore);
        }

        if (this.threadPool == null) {
            this.threadPool = new SimpleThreadPool(maxThreads, threadPriority);
        }

        schedulerPluginMap.put(CleanupSchedulerPlugin.class.getName(), new CleanupSchedulerPlugin(clusterService, jobDistributor, jobStore));

        DirectSchedulerFactory.getInstance().createScheduler(name, name, threadPool, jobStore, schedulerPluginMap, null, -1, -1, -1, false, null);

        // TODO well, change this somehow:

        Scheduler scheduler = DirectSchedulerFactory.getInstance().getScheduler(name);

        if (jobFactory != null) {
            scheduler.setJobFactory(jobFactory);
        }

        return scheduler;
    }

    private boolean isSchedulerPermanentlyDisabledForLocalNode() {
        // TODO ugh ... this is actually quite hard to decide
        return false;
    }

    private static class CleanupSchedulerPlugin implements SchedulerPlugin {

        private Scheduler scheduler;
        private JobDistributor jobDistributor;
        private ClusterService clusterService;
        private JobStore jobStore;

        CleanupSchedulerPlugin(ClusterService clusterService, JobDistributor jobDistributor, JobStore jobStore) {
            this.jobDistributor = jobDistributor;
            this.clusterService = clusterService;
            this.jobStore = jobStore;
        }

        @Override
        public void initialize(String name, Scheduler scheduler, ClassLoadHelper loadHelper) throws SchedulerException {
            this.scheduler = scheduler;

            if (this.clusterService != null) {

                this.clusterService.addLifecycleListener(new LifecycleListener() {
                    public void beforeStop() {
                        log.info("Shutting down scheduler " + CleanupSchedulerPlugin.this.scheduler + " because node is going down");

                        try {
                            jobStore.shutdown();
                        } catch (Exception e) {
                            log.error("Error while shutting down jobStore " + jobStore, e);
                        }

                        try {
                            // TODO make timeout configurable
                            Executors.newSingleThreadExecutor().submit(() -> shutdownScheduler()).get(10000, TimeUnit.MILLISECONDS);
                        } catch (TimeoutException e) {
                            log.error("Shutting down " + CleanupSchedulerPlugin.this.scheduler + " timed out", e);
                            log.error(getThreadDump());
                        } catch (Exception e) {
                            log.error("Error while shutting down scheduler " + CleanupSchedulerPlugin.this.scheduler, e);
                        }

                    }

                });
            }
        }

        public static String getThreadDump() {
            final StringBuilder dump = new StringBuilder();
            final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
            final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
            for (ThreadInfo threadInfo : threadInfos) {
                dump.append('"');
                dump.append(threadInfo.getThreadName());
                dump.append("\" ");
                final Thread.State state = threadInfo.getThreadState();
                dump.append("\n   java.lang.Thread.State: ");
                dump.append(state);
                final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
                for (final StackTraceElement stackTraceElement : stackTraceElements) {
                    dump.append("\n        at ");
                    dump.append(stackTraceElement);
                }
                dump.append("\n\n");
            }
            return dump.toString();
        }

        private void shutdownScheduler() {
            try {
                scheduler.shutdown(true);
                log.info("Shutdown complete");
            } catch (Exception e) {
                log.error("Error while shutting down scheduler " + CleanupSchedulerPlugin.this.scheduler, e);
            }
        }

        @Override
        public void start() {

        }

        @Override
        public void shutdown() {
            if (this.jobDistributor != null) {
                try {
                    this.jobDistributor.close();
                    this.jobDistributor = null;
                } catch (Exception e) {
                    log.warn("Error while closing jobDistributor", e);
                }
            }
        }
    }

    private static class DisabledScheduler implements Scheduler {

        private final String name;

        DisabledScheduler(String name) {
            this.name = name;
        }

        @Override
        public String getSchedulerName() throws SchedulerException {
            return name;
        }

        @Override
        public String getSchedulerInstanceId() throws SchedulerException {
            return null;
        }

        @Override
        public SchedulerContext getContext() throws SchedulerException {
            return new SchedulerContext();
        }

        @Override
        public void start() throws SchedulerException {

        }

        @Override
        public void startDelayed(int seconds) throws SchedulerException {

        }

        @Override
        public boolean isStarted() throws SchedulerException {
            return false;
        }

        @Override
        public void standby() throws SchedulerException {

        }

        @Override
        public boolean isInStandbyMode() throws SchedulerException {
            return false;
        }

        @Override
        public void shutdown() throws SchedulerException {

        }

        @Override
        public void shutdown(boolean waitForJobsToComplete) throws SchedulerException {

        }

        @Override
        public boolean isShutdown() throws SchedulerException {
            return true;
        }

        @Override
        public SchedulerMetaData getMetaData() throws SchedulerException {
            return new SchedulerMetaData(name, null, DisabledScheduler.class, false, false, false, true, null, 0, null, false, false, null, 0, "0");
        }

        @Override
        public List<JobExecutionContext> getCurrentlyExecutingJobs() throws SchedulerException {
            return Collections.emptyList();
        }

        @Override
        public void setJobFactory(JobFactory factory) throws SchedulerException {

        }

        @Override
        public ListenerManager getListenerManager() throws SchedulerException {
            return null;
        }

        @Override
        public Date scheduleJob(JobDetail jobDetail, Trigger trigger) throws SchedulerException {
            return null;
        }

        @Override
        public Date scheduleJob(Trigger trigger) throws SchedulerException {
            return null;
        }

        @Override
        public void scheduleJobs(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace) throws SchedulerException {

        }

        @Override
        public void scheduleJob(JobDetail jobDetail, Set<? extends Trigger> triggersForJob, boolean replace) throws SchedulerException {

        }

        @Override
        public boolean unscheduleJob(TriggerKey triggerKey) throws SchedulerException {
            return false;
        }

        @Override
        public boolean unscheduleJobs(List<TriggerKey> triggerKeys) throws SchedulerException {
            return false;
        }

        @Override
        public Date rescheduleJob(TriggerKey triggerKey, Trigger newTrigger) throws SchedulerException {
            return null;
        }

        @Override
        public void addJob(JobDetail jobDetail, boolean replace) throws SchedulerException {
        }

        @Override
        public void addJob(JobDetail jobDetail, boolean replace, boolean storeNonDurableWhileAwaitingScheduling) throws SchedulerException {
        }

        @Override
        public boolean deleteJob(JobKey jobKey) throws SchedulerException {
            return false;
        }

        @Override
        public boolean deleteJobs(List<JobKey> jobKeys) throws SchedulerException {
            return false;
        }

        @Override
        public void triggerJob(JobKey jobKey) throws SchedulerException {

        }

        @Override
        public void triggerJob(JobKey jobKey, JobDataMap data) throws SchedulerException {

        }

        @Override
        public void pauseJob(JobKey jobKey) throws SchedulerException {

        }

        @Override
        public void pauseJobs(GroupMatcher<JobKey> matcher) throws SchedulerException {

        }

        @Override
        public void pauseTrigger(TriggerKey triggerKey) throws SchedulerException {

        }

        @Override
        public void pauseTriggers(GroupMatcher<TriggerKey> matcher) throws SchedulerException {

        }

        @Override
        public void resumeJob(JobKey jobKey) throws SchedulerException {

        }

        @Override
        public void resumeJobs(GroupMatcher<JobKey> matcher) throws SchedulerException {

        }

        @Override
        public void resumeTrigger(TriggerKey triggerKey) throws SchedulerException {

        }

        @Override
        public void resumeTriggers(GroupMatcher<TriggerKey> matcher) throws SchedulerException {

        }

        @Override
        public void pauseAll() throws SchedulerException {

        }

        @Override
        public void resumeAll() throws SchedulerException {

        }

        @Override
        public List<String> getJobGroupNames() throws SchedulerException {
            return Collections.emptyList();
        }

        @Override
        public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws SchedulerException {
            return Collections.emptySet();
        }

        @Override
        public List<? extends Trigger> getTriggersOfJob(JobKey jobKey) throws SchedulerException {
            return Collections.emptyList();
        }

        @Override
        public List<String> getTriggerGroupNames() throws SchedulerException {
            return Collections.emptyList();
        }

        @Override
        public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) throws SchedulerException {
            return Collections.emptySet();
        }

        @Override
        public Set<String> getPausedTriggerGroups() throws SchedulerException {
            return Collections.emptySet();
        }

        @Override
        public JobDetail getJobDetail(JobKey jobKey) throws SchedulerException {
            return null;
        }

        @Override
        public Trigger getTrigger(TriggerKey triggerKey) throws SchedulerException {
            return null;
        }

        @Override
        public TriggerState getTriggerState(TriggerKey triggerKey) throws SchedulerException {
            return null;
        }

        @Override
        public void resetTriggerFromErrorState(TriggerKey triggerKey) throws SchedulerException {
        }

        @Override
        public void addCalendar(String calName, Calendar calendar, boolean replace, boolean updateTriggers) throws SchedulerException {
        }

        @Override
        public boolean deleteCalendar(String calName) throws SchedulerException {
            return false;
        }

        @Override
        public Calendar getCalendar(String calName) throws SchedulerException {
            return null;
        }

        @Override
        public List<String> getCalendarNames() throws SchedulerException {
            return Collections.emptyList();
        }

        @Override
        public boolean interrupt(JobKey jobKey) throws UnableToInterruptJobException {
            return false;
        }

        @Override
        public boolean interrupt(String fireInstanceId) throws UnableToInterruptJobException {
            return false;
        }

        @Override
        public boolean checkExists(JobKey jobKey) throws SchedulerException {
            return false;
        }

        @Override
        public boolean checkExists(TriggerKey triggerKey) throws SchedulerException {
            return false;
        }

        @Override
        public void clear() throws SchedulerException {

        }

    }

}