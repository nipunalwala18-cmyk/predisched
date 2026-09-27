package com.predisched.dashboard.web;

import com.predisched.dashboard.DashboardProperties;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.LongSupplier;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * Admin and chaos endpoints need the {@code X-Admin-Key} header (401 without it) and are rate
 * limited with a token bucket (429 when empty): {@code dashboard.adminRatePerSecond} per second,
 * bursts of twice that.
 */
@Component
public class AdminGuard implements HandlerInterceptor {

    public static final String HEADER = "X-Admin-Key";
    static final List<String> PROTECTED = List.of("/api/admin/**", "/api/chaos/**",
            "/api/models/*/promote", "/api/benchmarks/run");

    private final byte[] key;
    private final double ratePerSecond;
    private final double capacity;
    private final LongSupplier clock;
    private double tokens;
    private long lastRefillNanos;

    @org.springframework.beans.factory.annotation.Autowired
    public AdminGuard(DashboardProperties props) {
        this(props.getAdminKey(), props.getAdminRatePerSecond(), System::nanoTime);
    }

    AdminGuard(String key, double ratePerSecond, LongSupplier clock) {
        this.key = key.getBytes(StandardCharsets.UTF_8);
        this.ratePerSecond = ratePerSecond;
        this.capacity = Math.max(1, ratePerSecond * 2);
        this.clock = clock;
        this.tokens = capacity;
        this.lastRefillNanos = clock.getAsLong();
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response,
            Object handler) throws Exception {
        if ("OPTIONS".equals(request.getMethod())) {
            return true;
        }
        String given = request.getHeader(HEADER);
        if (given == null || !MessageDigest.isEqual(key, given.getBytes(StandardCharsets.UTF_8))) {
            response.setStatus(401);
            response.setContentType("application/json");
            response.getWriter().write("{\"error\":\"admin endpoints need the " + HEADER
                    + " header\"}");
            return false;
        }
        if (!take()) {
            response.setStatus(429);
            response.setHeader("Retry-After", "1");
            response.setContentType("application/json");
            response.getWriter().write("{\"error\":\"admin rate limit: at most "
                    + ratePerSecond + " calls per second\"}");
            return false;
        }
        return true;
    }

    /** A full bucket again (tests share one guard). */
    synchronized void reset() {
        tokens = capacity;
        lastRefillNanos = clock.getAsLong();
    }

    synchronized boolean take() {
        long now = clock.getAsLong();
        tokens = Math.min(capacity, tokens + (now - lastRefillNanos) / 1e9 * ratePerSecond);
        lastRefillNanos = now;
        if (tokens >= 1) {
            tokens -= 1;
            return true;
        }
        return false;
    }
}
