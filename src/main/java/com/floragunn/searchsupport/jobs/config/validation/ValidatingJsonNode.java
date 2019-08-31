package com.floragunn.searchsupport.jobs.config.validation;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.function.Function;

import org.apache.commons.validator.routines.EmailValidator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.floragunn.searchsupport.util.DurationFormat;

public class ValidatingJsonNode {
    private ValidationErrors validationErrors;
    private JsonNode jsonNode;

    public ValidatingJsonNode(JsonNode jsonNode, ValidationErrors validationErrors) {
        this.jsonNode = jsonNode;
        this.validationErrors = validationErrors;
    }

    public String requiredString(String attribute) {
        if (jsonNode.hasNonNull(attribute)) {
            return jsonNode.get(attribute).asText();
        } else {
            validationErrors.add(new MissingAttribute(attribute, jsonNode));
            return null;
        }
    }

    public int requiredInt(String attribute) {
        if (jsonNode.hasNonNull(attribute)) {
            JsonNode attributeNode = jsonNode.get(attribute);

            if (attributeNode.isNumber()) {
                return attributeNode.asInt();
            } else {
                validationErrors.add(new InvalidAttributeValue(attribute, attributeNode.toString(), "number", attributeNode));
                return 0;
            }
        } else {
            validationErrors.add(new MissingAttribute(attribute, jsonNode));
            return 0;
        }
    }

    public String string(String attribute) {
        if (jsonNode.hasNonNull(attribute)) {
            return jsonNode.get(attribute).asText();
        } else {
            return null;
        }
    }

    public String emailAddress(String attribute) {
        if (jsonNode.hasNonNull(attribute)) {
            String value = jsonNode.get(attribute).asText();

            if (EmailValidator.getInstance(true, true).isValid(value)) {
                return value;
            } else {
                validationErrors.add(new InvalidAttributeValue(attribute, value, "E-mail address", jsonNode));
                return null;
            }
        } else {
            return null;
        }
    }

    public List<String> emailAddressList(String attribute) {
        if (jsonNode.hasNonNull(attribute)) {
            JsonNode subNode = jsonNode.get(attribute);
            List<String> result;

            if (subNode.isArray()) {
                ArrayNode arrayNode = (ArrayNode) subNode;
                result = new ArrayList<>(arrayNode.size());

                for (JsonNode child : arrayNode) {
                    result.add(child.asText());
                }

            } else {
                result = Collections.singletonList(subNode.textValue());
            }

            int errorCount = 0;

            for (String address : result) {
                if (!EmailValidator.getInstance(true, true).isValid(address)) {
                    validationErrors.add(new InvalidAttributeValue(attribute, address, "E-mail address", jsonNode));
                    errorCount++;
                }
            }

            if (errorCount == 0) {
                return result;
            } else {
                return null;
            }

        } else {
            return null;
        }
    }

    public String[] emailAddressArray(String attribute) {
        List<String> result = emailAddressList(attribute);

        if (result != null) {
            return result.toArray(new String[result.size()]);
        } else {
            return null;
        }
    }

    public Integer intNumber(String attribute, Integer defaultValue) {
        if (jsonNode.hasNonNull(attribute)) {
            JsonNode attributeNode = jsonNode.get(attribute);

            if (attributeNode.isNumber()) {
                return attributeNode.asInt();
            } else {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
    }

    public Boolean booleanAttribute(String attribute, Boolean defaultValue) {
        if (jsonNode.hasNonNull(attribute)) {
            JsonNode attributeNode = jsonNode.get(attribute);

            if (attributeNode.isBoolean()) {
                return attributeNode.asBoolean();
            } else {
                return defaultValue;
            }
        } else {
            return defaultValue;
        }
    }

    public List<String> stringList(String attribute) {
        if (jsonNode.hasNonNull(attribute)) {
            JsonNode subNode = jsonNode.get(attribute);

            if (subNode.isArray()) {
                ArrayNode arrayNode = (ArrayNode) subNode;
                List<String> result = new ArrayList<>(arrayNode.size());

                for (JsonNode child : arrayNode) {
                    result.add(child.asText());
                }

                return result;
            } else {
                return Collections.singletonList(subNode.textValue());
            }

        } else {
            return null;
        }
    }

    public String[] stringArray(String attribute) {
        List<String> list = stringList(attribute);

        if (list != null) {
            return list.toArray(new String[list.size()]);
        } else {
            return null;
        }
    }

    public TimeZone timeZone(String attribute) {
        if (jsonNode.hasNonNull(attribute)) {
            String timeZoneId = jsonNode.get(attribute).asText();

            TimeZone timeZone = TimeZone.getTimeZone(timeZoneId);

            if (timeZone == null) {
                validationErrors.add(new InvalidAttributeValue(attribute, timeZoneId, TimeZone.class, jsonNode));
            }

            return timeZone;

        } else {
            return null;
        }
    }

    public Duration duration(String attribute) {
        if (jsonNode.hasNonNull(attribute)) {
            try {
                return DurationFormat.INSTANCE.parse(jsonNode.get(attribute).textValue());
            } catch (ConfigValidationException e) {
                validationErrors.add(attribute, e);
                return null;
            }
        } else {
            return null;
        }
    }

    public <R> R value(String attribute, Function<String, R> conversionFunction, Object expected, R defaultValue) {
        if (!jsonNode.hasNonNull(attribute)) {
            return defaultValue;
        }

        String value = jsonNode.get(attribute).asText();

        try {
            return conversionFunction.apply(value);
        } catch (Exception e) {
            validationErrors.add(new InvalidAttributeValue(attribute, value, expected, jsonNode).cause(e));
            return defaultValue;
        }
    }

    public <E extends Enum<E>> E caseInsensitiveEnum(String attribute, Class<E> enumClass, E defaultValue) {

        if (!jsonNode.hasNonNull(attribute)) {
            return defaultValue;
        }
        String value = jsonNode.get(attribute).asText();

        for (E e : enumClass.getEnumConstants()) {
            if (value.equalsIgnoreCase(e.name())) {
                return e;
            }
        }

        validationErrors.add(new InvalidAttributeValue(attribute, value, enumClass, jsonNode));

        return defaultValue;
    }

    public URI requiredURI(String attribute) {
        if (jsonNode.hasNonNull(attribute)) {
            String value = jsonNode.get(attribute).asText();

            try {
                return new URI(value);
            } catch (URISyntaxException e) {
                validationErrors.add(new InvalidAttributeValue(attribute, value, URI.class, jsonNode).message(e.getMessage()).cause(e));
                return null;
            }
        } else {
            validationErrors.add(new MissingAttribute(attribute, jsonNode));
            return null;
        }
    }

}
