package com.ingen.fleet.ingestion;

import java.util.Map;
import java.util.UUID;
import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpMediaTypeNotAcceptableException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.multipart.MultipartException;
import org.springframework.web.multipart.support.MissingServletRequestPartException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

/** Sanitized envelopes include framework-generated failures without reflecting request contents. */
@RestControllerAdvice
@ConditionalOnProperty(prefix = "fleet.ingestion", name = "enabled", havingValue = "true")
public class IngestionErrors {
    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ApiModels.ErrorEnvelope> api(ApiException error, HttpServletResponse response) {
        var result = ResponseEntity.status(error.status()).contentType(MediaType.APPLICATION_JSON);
        if (error.retryAfter() != null) { result.header(HttpHeaders.RETRY_AFTER, error.retryAfter().toString()); }
        return result.body(new ApiModels.ErrorEnvelope(error.code(), error.getMessage(), trace(response), error.details()));
    }

    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public ResponseEntity<ApiModels.ErrorEnvelope> tooLarge(HttpServletResponse response) {
        return envelope(413, "UPLOAD_TOO_LARGE", "Upload exceeds the configured request limit.", response);
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, MissingServletRequestPartException.class,
        MissingServletRequestParameterException.class, MultipartException.class})
    public ResponseEntity<ApiModels.ErrorEnvelope> malformed(Exception ignored, HttpServletResponse response) {
        return envelope(422, "INVALID_INPUT", "Required request fields are missing or malformed.", response);
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    public ResponseEntity<ApiModels.ErrorEnvelope> media(HttpServletResponse response) {
        return envelope(415, "UNSUPPORTED_MEDIA_TYPE", "Request media type is not supported by this endpoint.", response);
    }

    @ExceptionHandler(HttpMediaTypeNotAcceptableException.class)
    public ResponseEntity<ApiModels.ErrorEnvelope> accept(HttpServletResponse response) {
        return envelope(406, "NOT_ACCEPTABLE", "This endpoint returns application/json.", response);
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ApiModels.ErrorEnvelope> method(HttpServletResponse response) {
        return envelope(405, "METHOD_NOT_ALLOWED", "HTTP method is not supported by this endpoint.", response);
    }

    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<ApiModels.ErrorEnvelope> notFound(HttpServletResponse response) {
        return envelope(404, "NOT_FOUND", "The requested endpoint does not exist.", response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiModels.ErrorEnvelope> unexpected(HttpServletResponse response) {
        return envelope(500, "INTERNAL_ERROR", "The request could not be completed.", response);
    }

    static ResponseEntity<ApiModels.ErrorEnvelope> envelope(int status, String code,
            String message, HttpServletResponse response) {
        return ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON)
                .body(new ApiModels.ErrorEnvelope(code, message, trace(response), Map.of()));
    }

    static String trace(HttpServletResponse response) {
        String trace = MDC.get("trace_id");
        if (trace == null) { trace = response.getHeader("X-Trace-Id"); }
        if (trace == null || !trace.matches("[a-zA-Z0-9_-]{1,64}")) { trace = UUID.randomUUID().toString(); }
        response.setHeader("X-Trace-Id", trace);
        return trace;
    }

    @RestController
    @ConditionalOnProperty(prefix = "fleet.ingestion", name = "enabled", havingValue = "true")
    public static class Fallback implements ErrorController {
        @RequestMapping("/error")
        public ResponseEntity<ApiModels.ErrorEnvelope> error(HttpServletRequest request, HttpServletResponse response) {
            Object raw = request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE);
            int status = raw instanceof Integer value && value >= 400 && value <= 599 ? value : 500;
            return envelope(status, status == 404 ? "NOT_FOUND" : "REQUEST_FAILED",
                    status == 404 ? "The requested endpoint does not exist." : "The request could not be completed.", response);
        }
    }
}
