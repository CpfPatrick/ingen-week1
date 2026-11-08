package com.ingen.fleet;

import java.io.IOException;
import java.util.UUID;
import java.util.Map;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public class TraceFilter extends OncePerRequestFilter {
    private static final Logger LOG = LoggerFactory.getLogger(TraceFilter.class);
    static String safeTrace(String input) {
        return input != null && input.matches("[a-zA-Z0-9_-]{1,64}")
            ? input : UUID.randomUUID().toString();
    }
    @Override protected void doFilterInternal(HttpServletRequest request,
            HttpServletResponse response, FilterChain chain) throws ServletException, IOException {
        String trace = safeTrace(request.getHeader("X-Trace-Id"));
        Map<String, String> previousContext = MDC.getCopyOfContextMap();
        try {
            MDC.put("trace_id", trace);
            response.setHeader("X-Trace-Id", trace);
            chain.doFilter(request, response);
            MDC.put("trace_id", trace);
            LOG.atInfo().addKeyValue("status", response.getStatus()).log("request_completed");
        } catch (IOException | ServletException | RuntimeException | Error failure) {
            MDC.put("trace_id", trace);
            String kind = failure instanceof IOException ? "io"
                : failure instanceof ServletException ? "servlet"
                : failure instanceof RuntimeException ? "runtime" : "error";
            // A response may already be committed; status is its current value, not a
            // prediction of the container's eventual error response. Never attach failure.
            try {
                var event = LOG.atError().addKeyValue("failure_kind", kind);
                try {
                    event.addKeyValue("status", response.getStatus());
                } catch (RuntimeException | Error unavailableStatus) {
                    // Omit unavailable metadata but still emit the failure event.
                }
                event.log("request_failed");
            } catch (RuntimeException | Error loggingFailure) {
                // A broken logging backend must not replace the
                // original request failure. Do not print a raw fallback diagnostic.
            }
            throw failure;
        } finally {
            if (previousContext == null) { MDC.clear(); }
            else { MDC.setContextMap(previousContext); }
        }
    }
}
