package com.floragunn.searchsupport.jobs.config.validation;

import java.io.IOException;

import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.fasterxml.jackson.databind.JsonNode;

public class ValidationError implements ToXContentObject {
    private String attribute;
    private String message;
    private Exception cause;

    ValidationError(String attribute, String message) {
        this.attribute = attribute != null ? attribute : "_";
        this.message = message;
    }

    ValidationError(String attribute, String message, JsonNode jsonNode) {
        this.attribute = attribute != null ? attribute : "_";
        this.message = message;
    }

    public String getAttribute() {
        return attribute;
    }

    public String getMessage() {
        return message;
    }

    public ValidationError message(String message) {
        this.message = message;
        return this;
    }

    public ValidationError cause(Exception cause) {
        this.cause = cause;
        return this;
    }

    public Exception getCause() {
        return cause;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("error", message);
        builder.endObject();
        return builder;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    @Override
    public String toString() {
        return "ValidationError [message=" + message + ", cause=" + cause + "]";
    }
}