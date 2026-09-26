package com.predisched.fault;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class WorkerFailureDetectorTest {

    @Test
    void aWorkerIsDeclaredDeadOnTheThirdMissedHeartbeatNotBefore() {
        AtomicLong now = new AtomicLong(10_000);
        Map<String, Long> heartbeats = new ConcurrentHashMap<>(Map.of("w1", 10_000L, "w2", 10_000L));
        List<String> dead = new ArrayList<>();
        WorkerFailureDetector.Workers workers = new WorkerFailureDetector.Workers() {
            @Override
            public Map<String, Long> lastHeartbeats() {
                return heartbeats;
            }

            @Override
            public void declareDead(String workerId, long silentMs, int missed) {
                dead.add(workerId + " after " + missed);
                heartbeats.remove(workerId);
            }
        };
        try (WorkerFailureDetector detector =
                new WorkerFailureDetector(workers, 1_000, 3, now::get)) {
            now.set(12_999);           // two intervals missed
            heartbeats.put("w2", 12_900L);
            assertEquals(0, detector.check());

            now.set(13_000);           // w1: third interval missed; w2 heartbeated at 12.9 s
            assertEquals(1, detector.check());
            assertEquals(List.of("w1 after 3"), dead);

            now.set(20_000);
            assertEquals(1, detector.check());
            assertEquals(List.of("w1 after 3", "w2 after 7"), dead);
        }
    }
}
