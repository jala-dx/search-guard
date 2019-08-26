/*
 * Copyright 2015-2019 floragunn GmbH
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.floragunn.searchsupport.jobs.actions;

import java.io.IOException;
import java.util.Arrays;
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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.quartz.Scheduler;
import org.quartz.impl.SchedulerRepository;

import com.floragunn.searchguard.configuration.SearchGuardLicense;
import com.floragunn.searchguard.support.LicenseHelper;
import com.floragunn.searchsupport.jobs.SchedulerBuilder;
import com.floragunn.searchsupport.jobs.core.IndexJobStateStore;

public class TransportSchedulerConfigUpdateAction extends
        TransportNodesAction<SchedulerConfigUpdateRequest, SchedulerConfigUpdateResponse, TransportSchedulerConfigUpdateAction.NodeRequest, TransportSchedulerConfigUpdateAction.NodeResponse> {

    private final static Logger log = LogManager.getLogger(TransportSchedulerConfigUpdateAction.class);

    @Inject
    public TransportSchedulerConfigUpdateAction(final Settings settings, final ThreadPool threadPool, final ClusterService clusterService,
            final TransportService transportService, final ActionFilters actionFilters) {
        super(SchedulerConfigUpdateAction.NAME, threadPool, clusterService, transportService, actionFilters, SchedulerConfigUpdateRequest::new,
                TransportSchedulerConfigUpdateAction.NodeRequest::new, ThreadPool.Names.MANAGEMENT,
                TransportSchedulerConfigUpdateAction.NodeResponse.class);

    }

    protected NodeRequest newNodeRequest(final String nodeId, final SchedulerConfigUpdateRequest request) {
        return new NodeRequest(nodeId, request);
    }

    @Override
    protected NodeResponse newNodeResponse() {
        return new NodeResponse(clusterService.localNode(), null, null);
    }

    @Override
    protected SchedulerConfigUpdateResponse newResponse(SchedulerConfigUpdateRequest request, List<NodeResponse> responses,
            List<FailedNodeException> failures) {
        return new SchedulerConfigUpdateResponse(this.clusterService.getClusterName(), responses, failures);

    }

    @Override
    protected NodeResponse nodeOperation(final NodeRequest request) {
        DiscoveryNode localNode = clusterService.localNode();

        try {
            IndexJobStateStore<?> jobStore = IndexJobStateStore.getInstanceBySchedulerName(request.request.getSchedulerName());

            if (jobStore == null) {
                return new NodeResponse(localNode, NodeResponse.Status.NO_SUCH_JOB_STORE,
                        "A job store for scheduler name " + request.request.getSchedulerName() + " does not exist");
            }

            String status = jobStore.updateJobs();

            return new NodeResponse(localNode, NodeResponse.Status.SUCCESS, status);
        } catch (Exception e) {
            log.error("Error while updating jobs", e);
            return new NodeResponse(localNode, NodeResponse.Status.EXCEPTION, e.toString());
        }
    }

    public static class NodeRequest extends BaseNodeRequest {

        SchedulerConfigUpdateRequest request;

        public NodeRequest() {
        }

        public NodeRequest(final String nodeId, final SchedulerConfigUpdateRequest request) {
            super(nodeId);
            this.request = request;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            request = new SchedulerConfigUpdateRequest();
            request.readFrom(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    public static class NodeResponse extends BaseNodeResponse {

        private Status status;
        private String message;

        NodeResponse() {
        }

        public NodeResponse(final DiscoveryNode node, Status status, String message) {
            super(node);
            this.status = status;
            this.message = message;
        }

        public static TransportSchedulerConfigUpdateAction.NodeResponse readNodeResponse(StreamInput in) throws IOException {
            TransportSchedulerConfigUpdateAction.NodeResponse result = new TransportSchedulerConfigUpdateAction.NodeResponse();
            result.readFrom(in);
            return result;
        }

        public String getMessage() {
            return message;
        }

        public Status getStatus() {
            return status;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeEnum(status);
            out.writeOptionalString(message);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            status = in.readEnum(Status.class);
            message = in.readOptionalString();
        }

        @Override
        public String toString() {
            return "NodeResponse [status=" + status + ", message=" + message + "]";
        }

        public static enum Status {
            SUCCESS, NO_SUCH_JOB_STORE, EXCEPTION
        }
    }

}
