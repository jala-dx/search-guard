package com.floragunn.searchsupport.jobs.config.validation;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.floragunn.searchguard.DefaultObjectMapper;

public class ValidatingJsonParser {
    public static JsonNode readTree(String string) throws ConfigValidationException {
        try {
            return DefaultObjectMapper.readTree(string);
        } catch (JsonProcessingException e) {
            throw new ConfigValidationException(new ValidationError(null, "Error while parsing JSON document: " + e.getMessage(), null).cause(e));
        } catch (IOException e) {
            throw new ConfigValidationException(new ValidationError(null, "Error while parsing JSON document: " + e.getMessage(), null).cause(e));
        }
    }

}
