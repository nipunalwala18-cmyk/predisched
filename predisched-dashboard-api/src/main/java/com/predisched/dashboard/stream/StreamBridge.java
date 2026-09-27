package com.predisched.dashboard.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.predisched.dashboard.DashboardProperties;
import com.predisched.dashboard.cluster.Cluster;
import com.predisched.dashboard.cluster.Protos;
import com.predisched.proto.EventMessage;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * The cluster to the browser (prompt 22). A background thread follows the primary's
 * {@code StreamEvents} and sorts each event into a topic (decisions, task lifecycle, everything
 * else to events); every second the primary's worker state goes to {@code metrics}. The
 * {@link Throttler} sends the batches. A broken stream (a failover) reconnects to the new
 * primary.
 */
@Component
public class StreamBridge {

    private static final Logger log = LoggerFactory.getLogger(StreamBridge.class);
    static final Set<String> TASK_EVENTS = Set.of("SUBMIT", "ENQUEUE", "DISPATCH",
            "EXECUTE_START", "EXECUTE_END", "RESULT", "TIMEOUT", "RETRY_SCHEDULED", "DEAD_LETTER",
            "SPECULATE", "SPECULATION_RESULT", "PREDICTION_OUTCOME");

    private final Cluster cluster;
    private final ObjectMapper json = new ObjectMapper();
    private final Throttler throttler;
    private final long tickMs;
    private final ScheduledExecutorService timers = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "stream-bridge");
        t.setDaemon(true);
        return t;
    });
    private volatile boolean running;
    private Thread follower;

    public StreamBridge(Cluster cluster, SimpMessagingTemplate template,
            DashboardProperties props) {
        this.cluster = cluster;
        this.throttler = new Throttler((topic, batch) ->
                template.convertAndSend("/topic/" + topic, batch), 500);
        this.tickMs = Math.max(50, 1000L / Math.max(1, props.getStreamRatePerSecond()));
    }

    public Throttler throttler() {
        return throttler;
    }

    /** Queues an item for a topic (sent with the next tick). */
    public void publish(String topic, Object item) {
        throttler.add(topic, item);
    }

    @PostConstruct
    void start() {
        running = true;
        timers.scheduleAtFixedRate(throttler::flush, tickMs, tickMs, TimeUnit.MILLISECONDS);
        timers.scheduleAtFixedRate(this::pollMetrics, 1_000, 1_000, TimeUnit.MILLISECONDS);
        follower = new Thread(this::follow, "event-follower");
        follower.setDaemon(true);
        follower.start();
    }

    @PreDestroy
    void stop() {
        running = false;
        timers.shutdownNow();
        if (follower != null) {
            follower.interrupt();
        }
    }

    private void pollMetrics() {
        try {
            Map<String, Object> state = Protos.state(cluster.state());
            publish("metrics", Map.of("atMs", state.get("atMs"), "workers", state.get("workers"),
                    "queueDepth", state.get("queueDepth"), "running", state.get("running")));
        } catch (RuntimeException e) {
            // cluster down: the UI shows the gap
        }
    }

    private void follow() {
        while (running) {
            try {
                Iterator<EventMessage> events = cluster.streamEvents();
                log.info("Following the primary's event stream");
                while (running && events.hasNext()) {
                    route(events.next().getJson());
                }
            } catch (RuntimeException e) {
                log.debug("Event stream broken ({}); reconnecting", e.toString());
            }
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    @SuppressWarnings("unchecked")
    void route(String line) {
        Map<String, Object> event;
        try {
            event = json.readValue(line, Map.class);
        } catch (Exception e) {
            return;
        }
        String type = String.valueOf(event.get("type"));
        if ("SCHEDULE_DECISION".equals(type)) {
            publish("decisions", event);
        } else if (TASK_EVENTS.contains(type)) {
            publish("tasks", event);
        } else {
            publish("events", event);
        }
    }

    List<String> topics() {
        return List.of("metrics", "tasks", "events", "decisions");
    }
}
