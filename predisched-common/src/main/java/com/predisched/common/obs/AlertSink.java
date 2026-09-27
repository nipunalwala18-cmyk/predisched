package com.predisched.common.obs;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alerts (prompt 21, F23 minimal): {@code DRIFT}, {@code SLA_BREACH} and {@code NODE_FAILURE}
 * posted as one JSON object to a webhook, asynchronously, so an alert never slows the scheduler.
 * No webhook configured: a no-op. At most one alert per type every {@code minIntervalMs}; the
 * rest are counted as suppressed and reported with the next one sent.
 */
public final class AlertSink {

    private static final Logger log = LoggerFactory.getLogger(AlertSink.class);

    public static final String DRIFT = "DRIFT";
    public static final String SLA_BREACH = "SLA_BREACH";
    public static final String NODE_FAILURE = "NODE_FAILURE";

    private static volatile AlertSink instance = new AlertSink("", "", 0, System::currentTimeMillis);

    private final String nodeId;
    private final URI webhook;
    private final long minIntervalMs;
    private final LongSupplier clock;
    private final HttpClient http;
    private final Map<String, Long> lastSent = new ConcurrentHashMap<>();
    private final Map<String, Integer> suppressed = new ConcurrentHashMap<>();

    public AlertSink(String nodeId, String webhookUrl, long minIntervalMs, LongSupplier clock) {
        this.nodeId = nodeId;
        this.webhook = webhookUrl == null || webhookUrl.isBlank() ? null : URI.create(webhookUrl);
        this.minIntervalMs = minIntervalMs;
        this.clock = clock;
        this.http = webhook == null ? null : HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2)).build();
    }

    public static AlertSink install(AlertSink sink) {
        instance = sink;
        if (sink.enabled()) {
            log.info("Alerts go to {}", sink.webhook);
        }
        return sink;
    }

    public static AlertSink get() {
        return instance;
    }

    public boolean enabled() {
        return webhook != null;
    }

    /** The JSON body of one alert. */
    public String body(String type, Map<String, String> details, int suppressedBefore) {
        String fields = details.entrySet().stream()
                .map(e -> "\"" + escape(e.getKey()) + "\":\"" + escape(e.getValue()) + "\"")
                .collect(Collectors.joining(","));
        return "{\"type\":\"" + escape(type) + "\",\"node\":\"" + escape(nodeId)
                + "\",\"at\":\"" + Instant.ofEpochMilli(clock.getAsLong()) + "\""
                + ",\"suppressed_since_last\":" + suppressedBefore
                + (fields.isEmpty() ? "" : ",\"details\":{" + fields + "}") + "}";
    }

    /** Sends the alert unless the type sent one too recently; returns whether it was sent. */
    public boolean alert(String type, Map<String, String> details) {
        if (webhook == null) {
            return false;
        }
        long now = clock.getAsLong();
        Long last = lastSent.get(type);
        if (last != null && now - last < minIntervalMs) {
            suppressed.merge(type, 1, Integer::sum);
            return false;
        }
        lastSent.put(type, now);
        int before = suppressed.getOrDefault(type, 0);
        suppressed.put(type, 0);
        HttpRequest request = HttpRequest.newBuilder(webhook)
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body(type, details, before)))
                .build();
        http.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .whenComplete((response, error) -> {
                    if (error != null) {
                        log.warn("Alert {} not delivered to {}: {}", type, webhook,
                                error.toString());
                    } else if (response.statusCode() >= 300) {
                        log.warn("Alert {} rejected by {}: HTTP {}", type, webhook,
                                response.statusCode());
                    }
                });
        return true;
    }

    private static String escape(String text) {
        return text == null ? "" : text.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n");
    }
}
