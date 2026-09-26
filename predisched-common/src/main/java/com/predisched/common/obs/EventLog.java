package com.predisched.common.obs;

import com.predisched.common.time.Clocks;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The node's structured event stream: one JSON object per line in {@code logs/<node>.jsonl}
 * (spec §15.8). Every event carries the node id, its Lamport time and its physical time, which is
 * what lets `scripts/merge-events.py` order the whole cluster's events two different ways.
 *
 * <p>Writes are serialised on this object and flushed per line: the volumes here are small, and a
 * missing tail would spoil the ordering demo. Prompt 11 moves the durable copy into PostgreSQL.
 */
public class EventLog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(EventLog.class);

    /** Event types emitted along a task's path, so the demo shows a causal chain. */
    public static final String SUBMIT = "SUBMIT";
    public static final String ENQUEUE = "ENQUEUE";
    public static final String DISPATCH = "DISPATCH";
    public static final String EXECUTE_START = "EXECUTE_START";
    public static final String EXECUTE_END = "EXECUTE_END";
    public static final String RESULT = "RESULT";
    public static final String CLOCK_SYNC = "CLOCK_SYNC";
    /** Election events (prompt 06): a node starts an election, and a node learns the leader. */
    public static final String ELECTION_START = "ELECTION_START";
    public static final String LEADER_ELECTED = "LEADER_ELECTED";
    /** A scheduling strategy picked a worker for a task (prompt 08). */
    public static final String SCHEDULE_DECISION = "SCHEDULE_DECISION";

    private static volatile EventLog instance = new EventLog();

    private final String nodeId;
    private final BufferedWriter writer;

    /** A no-op log, used until a node installs a real one (tests, the CLI client). */
    private EventLog() {
        this.nodeId = "unset";
        this.writer = null;
    }

    public EventLog(String nodeId, Path file) {
        this.nodeId = nodeId;
        try {
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            this.writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open event log " + file, e);
        }
    }

    /** Opens {@code logs/<nodeId>.jsonl} and makes it this JVM's event log. */
    public static EventLog install(String nodeId) {
        EventLog opened = new EventLog(nodeId, Paths.get("logs", nodeId + ".jsonl"));
        instance = opened;
        return opened;
    }

    public static EventLog get() {
        return instance;
    }

    public void event(String type, String taskId, Map<String, String> details) {
        if (writer == null) {
            return;
        }
        // Recording an event is a local event, so it advances the logical clock. Without this
        // several events on one node would share a timestamp and the Lamport ordering would be
        // unable to separate them.
        long lamport = Clocks.lamport().tick();
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("node", nodeId);
        fields.put("lamport", lamport);
        fields.put("physical_ms", Clocks.now());
        fields.put("wall_ms", System.currentTimeMillis());
        fields.put("type", type);
        fields.put("task_id", taskId == null ? "" : taskId);
        fields.put("trace", TraceContext.current());
        if (details != null) {
            details.forEach(fields::put);
        }
        String line = toJson(fields);
        synchronized (this) {
            try {
                writer.write(line);
                writer.newLine();
                writer.flush();
            } catch (IOException e) {
                log.warn("Could not write event {}: {}", type, e.getMessage());
            }
        }
    }

    public void event(String type, String taskId) {
        event(type, taskId, Map.of());
    }

    private static String toJson(Map<String, Object> fields) {
        StringBuilder out = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            if (!first) {
                out.append(',');
            }
            first = false;
            out.append('"').append(escape(entry.getKey())).append("\":");
            Object value = entry.getValue();
            if (value instanceof Number || value instanceof Boolean) {
                out.append(value);
            } else {
                out.append('"').append(escape(String.valueOf(value))).append('"');
            }
        }
        return out.append('}').toString();
    }

    private static String escape(String value) {
        StringBuilder out = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }

    @Override
    public void close() {
        if (writer == null) {
            return;
        }
        synchronized (this) {
            try {
                writer.close();
            } catch (IOException e) {
                log.warn("Could not close event log: {}", e.getMessage());
            }
        }
    }
}
