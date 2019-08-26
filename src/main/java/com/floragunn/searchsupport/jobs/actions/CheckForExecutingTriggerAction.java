package com.floragunn.searchsupport.jobs.actions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.StreamableResponseActionType;

public class CheckForExecutingTriggerAction extends StreamableResponseActionType<CheckForExecutingTriggerResponse> {
    private final static Logger log = LogManager.getLogger(CheckForExecutingTriggerAction.class);

    public static final CheckForExecutingTriggerAction INSTANCE = new CheckForExecutingTriggerAction();
    public static final String NAME = "cluster:admin/searchsupport/scheduler/executing_triggers/check";

    protected CheckForExecutingTriggerAction() {
        super(NAME);
    }

    @Override
    public CheckForExecutingTriggerResponse newResponse() {
        return new CheckForExecutingTriggerResponse();
    }

    /*
    public static void send(Client client, String schedulerName) {
        client.execute(CheckForExecutingTriggerAction.INSTANCE, new CheckForExecutingTriggerRequest(schedulerName),
                new ActionListener<CheckForExecutingTriggerResponse>() {

                    @Override
                    public void onResponse(CheckForExecutingTriggerResponse response) {
                        log.info("Result of scheduler config update of " + schedulerName + ":\n" + response);

                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error("Scheduler config update of " + schedulerName + " failed", e);
                    }
                });
    }

*/
}
