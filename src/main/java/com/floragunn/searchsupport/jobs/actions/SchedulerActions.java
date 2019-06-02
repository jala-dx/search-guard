package com.floragunn.searchsupport.jobs.actions;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;

public class SchedulerActions {
    public static List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(new ActionHandler<>(SchedulerConfigUpdateAction.INSTANCE, TransportSchedulerConfigUpdateAction.class));
        return actions;
    }
}
