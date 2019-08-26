package com.floragunn.searchsupport.jobs.config.validation;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;

public class ConfigValidationException extends Exception {

    private static final long serialVersionUID = 8874565903177850402L;

    private ValidationErrors validationErrors;

    public ConfigValidationException(ValidationErrors validationErrors) {
        super(getMessage(validationErrors));

        this.validationErrors = validationErrors;
    }

    public ConfigValidationException(Multimap<String, ValidationError> validationErrors) {
        this(new ValidationErrors(validationErrors));
    }

    public ConfigValidationException(ValidationError validationError) {
        this(ImmutableListMultimap.of(validationError.getAttribute(), validationError));
    }

    public ValidationErrors getValidationErrors() {
        return validationErrors;
    }

    private static String getMessage(ValidationErrors validationErrors) {
        int size = validationErrors.size();

        if (size == 1) {
            ValidationError onlyError = validationErrors.getOnlyValidationError();

            if (onlyError.getAttribute() != null) {
                return "'" + onlyError.getAttribute() + "': " + onlyError.getMessage();
            } else {
                return onlyError.getMessage();
            }

        } else {
            return size + " errors; see detail.";
        }
    }

    public String toString() {
        return "ConfigValidationException: " + this.getMessage() + "\n" + this.validationErrors;
    }

}
