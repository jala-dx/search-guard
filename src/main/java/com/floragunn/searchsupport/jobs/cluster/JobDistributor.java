package com.floragunn.searchsupport.jobs.cluster;

import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;

import com.floragunn.searchsupport.jobs.config.JobConfig;

public class JobDistributor implements AutoCloseable {
    protected final Logger log = LogManager.getLogger(this.getClass());

    private final String name;
    private final String nodeFilter;
    private final String[] nodeFilterElements;
    private final ClusterService clusterService;
    private final NodeComparator<?> nodeComparator;
    private DistributedJobStore distributedJobStore;
    private int availableNodes = 0;
    private int currentNodeIndex = -1;

    public JobDistributor(String name, String nodeFilter, ClusterService clusterService, DistributedJobStore distributedJobStore) {
        this(name, nodeFilter, clusterService, distributedJobStore, new NodeIdComparator(clusterService));
    }

    public JobDistributor(String name, String nodeFilter, ClusterService clusterService, DistributedJobStore distributedJobStore,
            NodeComparator<?> nodeComparator) {
        this.name = name;
        this.nodeFilter = nodeFilter;
        this.nodeFilterElements = nodeFilter != null ? nodeFilter.split(",") : null;
        this.clusterService = clusterService;
        this.distributedJobStore = distributedJobStore;
        this.nodeComparator = nodeComparator;

        init();
    }

    public boolean isJobSelected(JobConfig jobConfig) {
        return this.isJobSelected(jobConfig, this.currentNodeIndex);
    }

    public boolean isJobSelected(JobConfig jobConfig, int nodeIndex) {
        int jobNodeIndex = Math.abs(jobConfig.hashCode()) % this.availableNodes;

        if (jobNodeIndex == nodeIndex) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "JobDistributor " + name;
    }

    @Override
    public void close() throws Exception {
        this.clusterService.removeListener(clusterStateListener);
    }

    private void init() {
        clusterService.addListener(clusterStateListener);
        update(clusterService.state());
    }

    private boolean update(ClusterState clusterState) {
        int oldAvailableNodes = this.availableNodes;
        int oldCurrentNodeIndex = this.currentNodeIndex;
        Object[] availableNodeIds = getAvailableNodeIds(clusterState);

        if (log.isDebugEnabled()) {
            log.debug("Update of " + this + " on " + clusterState.nodes().getLocalNodeId() + ": " + Arrays.asList(availableNodeIds));
        }

        this.availableNodes = availableNodeIds.length;

        if (this.availableNodes == 0) {
            log.error("No nodes available for " + this + "\nnodeFilter: " + nodeFilter);
            this.currentNodeIndex = -1;
        } else {
            this.currentNodeIndex = Arrays.binarySearch(availableNodeIds, this.nodeComparator.resolveNodeId(clusterState.nodes().getLocalNodeId()));
        }

        if (oldAvailableNodes == this.availableNodes && oldCurrentNodeIndex == this.currentNodeIndex) {
            log.debug("Cluster state change does not require rescheduling of jobs");
            return false;
        }

        if (currentNodeIndex == -1) {
            if (log.isDebugEnabled()) {
                log.debug("The current node is not configured to execute jobs for " + this + "\nnodeFilter: " + nodeFilter);
            }
            return true;
        }

        if (log.isDebugEnabled()) {
            log.debug("Node index of " + clusterState.nodes().getLocalNodeId() + " after update: " + currentNodeIndex);
        }

        return true;
    }

    private Object[] getAvailableNodeIds(ClusterState clusterState) {
        Object[] nodeIds = this.nodeComparator.resolveNodeFilters(this.nodeFilterElements);

        Arrays.sort(nodeIds);

        return nodeIds;
    }

    private final ClusterStateListener clusterStateListener = new ClusterStateListener() {

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            if (update(event.state()) && distributedJobStore != null) {
                distributedJobStore.clusterConfigChanged();
            }
        }

    };

    public DistributedJobStore getDistributedJobStore() {
        return distributedJobStore;
    }

    public void setDistributedJobStore(DistributedJobStore distributedJobStore) {
        this.distributedJobStore = distributedJobStore;
    }

}
