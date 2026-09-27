package com.predisched.common.obs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Alerts go to the webhook as JSON, at most one per type per interval; no URL, no alerts. */
class AlertSinkTest {

    @Test
    void postsJsonAndRateLimitsPerType() throws Exception {
        List<String> bodies = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/hook", exchange -> {
            bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });
        server.start();
        try {
            AtomicLong now = new AtomicLong(1_000_000);
            AlertSink sink = new AlertSink("scheduler-1", "http://127.0.0.1:"
                    + server.getAddress().getPort() + "/hook", 5_000, now::get);
            assertTrue(sink.alert(AlertSink.DRIFT, Map.of("ratio", "2.1")));
            assertFalse(sink.alert(AlertSink.DRIFT, Map.of("ratio", "2.2")), "too soon");
            assertTrue(sink.alert(AlertSink.NODE_FAILURE, Map.of("worker", "worker-2")),
                    "another type is not held back");
            now.addAndGet(5_000);
            assertTrue(sink.alert(AlertSink.DRIFT, Map.of("ratio", "2.3")));
            long deadline = System.currentTimeMillis() + 5_000;
            while (bodies.size() < 3 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(3, bodies.size());
            String last = bodies.stream().filter(b -> b.contains("2.3")).findFirst().orElseThrow();
            assertTrue(last.startsWith("{\"type\":\"DRIFT\",\"node\":\"scheduler-1\""), last);
            assertTrue(last.contains("\"suppressed_since_last\":1"), last);
            assertTrue(last.contains("\"details\":{\"ratio\":\"2.3\"}"), last);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void withoutAWebhookNothingIsSent() {
        AlertSink sink = new AlertSink("n", "", 0, System::currentTimeMillis);
        assertFalse(sink.enabled());
        assertFalse(sink.alert(AlertSink.SLA_BREACH, Map.of()));
    }
}
