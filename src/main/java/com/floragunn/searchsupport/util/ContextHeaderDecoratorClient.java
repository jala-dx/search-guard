package com.floragunn.searchsupport.util;

import java.util.Collections;
import java.util.Map;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;

public class ContextHeaderDecoratorClient extends FilterClient {

    private Map<String, String> headers;

    public ContextHeaderDecoratorClient(Client in, Map<String, String> headers) {
        super(in);
        this.headers = headers != null ? headers : Collections.emptyMap();
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(Action<Response> action, Request request,
            ActionListener<Response> listener) {

        ThreadContext threadContext = threadPool().getThreadContext();

        try (StoredContext ctx = threadContext.stashContext()) {
            threadContext.putHeader(this.headers);

            super.doExecute(action, request, listener);
        }
    }

}
