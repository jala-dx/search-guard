package com.floragunn.searchsupport.jobs.cluster;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;

import com.floragunn.searchsupport.jobs.config.JobConfig;

public class JobDistributor {
    protected final Logger log = LogManager.getLogger(this.getClass());

    private final String name;
    private final String nodeFilter;
    private int availableNodes;
    private int currentNodeIndex;

    public JobDistributor(String name, String nodeFilter) {
        this.name = name;
        this.nodeFilter = nodeFilter;
    }

    public void init(ClusterService clusterService) {
        clusterService.addListener(clusterStateListener);
    }

    @Override
    public String toString() {
        return "JobExecutionEngine " + name;
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

    private boolean update(ClusterState clusterState) {
        String[] availableNodeIds = getAvailableNodeIds(clusterState, nodeFilter);

        if (log.isDebugEnabled()) {
            log.debug("Update of " + this + ": " + Arrays.asList(availableNodeIds));
        }

        this.availableNodes = availableNodeIds.length;

        if (this.availableNodes == 0) {
            log.error("No nodes available for " + this + "\nnodeFilter: " + nodeFilter);
            return false;
        }

        this.currentNodeIndex = Arrays.binarySearch(availableNodeIds, clusterState.nodes().getLocalNodeId());

        if (currentNodeIndex == -1) {
            if (log.isDebugEnabled()) {
                log.debug("The current node is not configured to execute jobs for " + this + "\nnodeFilter: " + nodeFilter);
            }
            return false;
        }

        return true;
    }

    private String[] getAvailableNodeIds(ClusterState clusterState, String nodeFilter) {
        String[] nodeIds = clusterState.nodes().resolveNodes(this.nodeFilter.split(","));

        Arrays.sort(nodeIds);

        return nodeIds;
    }

    private final ClusterStateListener clusterStateListener = new ClusterStateListener() {

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            update(event.state());
        }

    };
}
