package com.floragunn.searchsupport.jobs.config.validation;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.ScriptException;

public class ScriptValidationError extends ValidationError {

    public ScriptValidationError(String attribute, ScriptException scriptException) {
        super(attribute, scriptException.getMessage());
        cause(scriptException);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("error", getMessage());
        builder.endObject();
        return builder;
    }

}