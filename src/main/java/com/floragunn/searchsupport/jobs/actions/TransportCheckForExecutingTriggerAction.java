package com.floragunn.searchsupport.jobs.actions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.impl.DirectSchedulerFactory;

public class TransportCheckForExecutingTriggerAction extends
        TransportNodesAction<CheckForExecutingTriggerRequest, CheckForExecutingTriggerResponse, TransportCheckForExecutingTriggerAction.NodeRequest, TransportCheckForExecutingTriggerAction.NodeResponse> {

    private final static Logger log = LogManager.getLogger(TransportCheckForExecutingTriggerAction.class);

    @Inject
    public TransportCheckForExecutingTriggerAction(final Settings settings, final ThreadPool threadPool, final ClusterService clusterService,
            final TransportService transportService, final ActionFilters actionFilters) {
        super(CheckForExecutingTriggerAction.NAME, threadPool, clusterService, transportService, actionFilters, CheckForExecutingTriggerRequest::new,
                TransportCheckForExecutingTriggerAction.NodeRequest::new, ThreadPool.Names.MANAGEMENT,
                TransportCheckForExecutingTriggerAction.NodeResponse.class);

    }

    protected NodeRequest newNodeRequest(final String nodeId, final CheckForExecutingTriggerRequest request) {
        return new NodeRequest(nodeId, request);
    }

    @Override
    protected NodeResponse newNodeResponse() {
        return new NodeResponse(clusterService.localNode(), new ArrayList<>());
    }

    @Override
    protected CheckForExecutingTriggerResponse newResponse(CheckForExecutingTriggerRequest request, List<NodeResponse> responses,
            List<FailedNodeException> failures) {
        return new CheckForExecutingTriggerResponse(this.clusterService.getClusterName(), responses, failures);

    }

    @Override
    protected NodeResponse nodeOperation(final NodeRequest request) {
        DiscoveryNode localNode = clusterService.localNode();

        try {
            Scheduler scheduler = DirectSchedulerFactory.getInstance().getScheduler(request.request.getSchedulerName());

            if (scheduler == null) {
                return new NodeResponse(clusterService.localNode(), new ArrayList<>());
            }

            List<JobExecutionContext> executingJobs = scheduler.getCurrentlyExecutingJobs();

            if (executingJobs.size() == 0) {
                return new NodeResponse(clusterService.localNode(), new ArrayList<>());
            }

            HashSet<String> requestedTriggerKeys = new HashSet<>(request.request.getTriggerKeys());
            ArrayList<String> foundTriggerKeys = new ArrayList<>(requestedTriggerKeys.size());

            for (JobExecutionContext executingJob : executingJobs) {
                if (executingJob.getTrigger() == null) {
                    continue;
                }

                String triggerKey = executingJob.getTrigger().getKey().toString();

                if (requestedTriggerKeys.contains(triggerKey)) {
                    foundTriggerKeys.add(triggerKey);
                }
            }

            return new NodeResponse(localNode, foundTriggerKeys);
        } catch (Exception e) {
            log.error("Error while retrieving running triggers", e);
            return new NodeResponse(localNode, new ArrayList<>(), e.toString());
        }
    }

    public static class NodeRequest extends BaseNodeRequest {

        CheckForExecutingTriggerRequest request;

        public NodeRequest() {
        }

        public NodeRequest(final String nodeId, final CheckForExecutingTriggerRequest request) {
            super(nodeId);
            this.request = request;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            request = new CheckForExecutingTriggerRequest();
            request.readFrom(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    public static class NodeResponse extends BaseNodeResponse {

        private List<String> executingTriggers;
        private String message;

        NodeResponse() {
        }

        public NodeResponse(final DiscoveryNode node, List<String> executingTriggers) {
            super(node);
            this.executingTriggers = executingTriggers;
        }

        public NodeResponse(final DiscoveryNode node, List<String> executingTriggers, String message) {
            super(node);
            this.executingTriggers = executingTriggers;
            this.message = message;
        }

        public static TransportCheckForExecutingTriggerAction.NodeResponse readNodeResponse(StreamInput in) throws IOException {
            TransportCheckForExecutingTriggerAction.NodeResponse result = new TransportCheckForExecutingTriggerAction.NodeResponse();
            result.readFrom(in);
            return result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringCollection(executingTriggers);
            out.writeOptionalString(message);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            executingTriggers = in.readStringList();
            message = in.readOptionalString();
        }

        public List<String> getExecutingTriggers() {
            return executingTriggers;
        }

        public void setExecutingTriggers(List<String> executingTriggers) {
            this.executingTriggers = executingTriggers;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        @Override
        public String toString() {
            return "NodeResponse [executingTriggers=" + executingTriggers + ", message=" + message + "]";
        }

    }

}
