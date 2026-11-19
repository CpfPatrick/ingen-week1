package com.ingen.fleet.ingestion;

import java.util.Map;

public class ApiException extends RuntimeException {
    private final int status;
    private final String code;
    private final Map<String, Object> details;
    private final Integer retryAfter;

    public ApiException(int status, String code, String message) {
        this(status, code, message, Map.of(), null);
    }

    public ApiException(int status, String code, String message,
                        Map<String, Object> details, Integer retryAfter) {
        super(message);
        this.status = status;
        this.code = code;
        this.details = Map.copyOf(details);
        this.retryAfter = retryAfter;
    }

    public int status() { return status; }
    public String code() { return code; }
    public Map<String, Object> details() { return details; }
    public Integer retryAfter() { return retryAfter; }

    public static ApiException invalid(String field, String message) {
        return new ApiException(422, "INVALID_INPUT", message, Map.of("field", field), null);
    }

    public static ApiException unavailable() {
        return new ApiException(503, "DEPENDENCY_UNAVAILABLE",
                "An ingestion dependency is unavailable; retry the same request.");
    }
}
