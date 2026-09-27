package com.predisched.dashboard.stream;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Batches stream items per topic and releases one batch per topic per tick (prompt 22): at a
 * tick of 250 ms a topic gets at most 4 messages a second however fast the cluster talks. A batch
 * holds at most {@code maxBatch} items, the newest; older ones are counted as dropped. Spec §15.11:
 * a 60 Hz stream makes the browser stutter.
 */
public final class Throttler {

    private final BiConsumer<String, List<Object>> sender;
    private final int maxBatch;
    private final Map<String, List<Object>> buffers = new LinkedHashMap<>();
    private long dropped;

    public Throttler(BiConsumer<String, List<Object>> sender, int maxBatch) {
        this.sender = sender;
        this.maxBatch = maxBatch;
    }

    public synchronized void add(String topic, Object item) {
        List<Object> buffer = buffers.computeIfAbsent(topic, t -> new ArrayList<>());
        buffer.add(item);
        if (buffer.size() > maxBatch) {
            buffer.remove(0);
            dropped++;
        }
    }

    /** One tick: every topic with something waiting sends it as one message. */
    public void flush() {
        Map<String, List<Object>> ready;
        synchronized (this) {
            ready = new LinkedHashMap<>();
            buffers.forEach((topic, items) -> {
                if (!items.isEmpty()) {
                    ready.put(topic, new ArrayList<>(items));
                    items.clear();
                }
            });
        }
        ready.forEach(sender);
    }

    public synchronized long dropped() {
        return dropped;
    }
}
