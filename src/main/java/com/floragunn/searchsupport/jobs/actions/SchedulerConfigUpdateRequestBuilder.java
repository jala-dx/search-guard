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

import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.ElasticsearchClient;

public class SchedulerConfigUpdateRequestBuilder
        extends NodesOperationRequestBuilder<SchedulerConfigUpdateRequest, SchedulerConfigUpdateResponse, SchedulerConfigUpdateRequestBuilder> {
    public SchedulerConfigUpdateRequestBuilder(final ClusterAdminClient client) {
        this(client, SchedulerConfigUpdateAction.INSTANCE);
    }

    public SchedulerConfigUpdateRequestBuilder(final ElasticsearchClient client, final SchedulerConfigUpdateAction action) {
        super(client, action, new SchedulerConfigUpdateRequest());
    }

    public SchedulerConfigUpdateRequestBuilder setShardId(final String schedulerName) {
        request().setSchedulerName(schedulerName);
        return this;
    }
}
