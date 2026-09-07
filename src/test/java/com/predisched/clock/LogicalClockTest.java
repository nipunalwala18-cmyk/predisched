package com.predisched.clock;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class LogicalClockTest {

    @Test
    @DisplayName("1. Test initial clock value is 0")
    public void testInitialClockValue() {
        LogicalClock clock = new LogicalClock("Node-1");
        assertEquals(0L, clock.getTime());
        assertEquals("Node-1", clock.getNodeId());
    }

    @Test
    @DisplayName("2. Test local event increments clock by 1")
    public void testLocalEventIncrementsClock() {
        LogicalClock clock = new LogicalClock("Node-1");
        long t1 = clock.localEvent();
        assertEquals(1L, t1);
        assertEquals(1L, clock.getTime());

        long t2 = clock.increment();
        assertEquals(2L, t2);
        assertEquals(2L, clock.getTime());
    }

    @Test
    @DisplayName("3. Test send event increments clock by 1")
    public void testSendEventIncrementsClock() {
        LogicalClock clock = new LogicalClock("Node-1");
        long sendTime = clock.sendEvent();
        assertEquals(1L, sendTime);
        assertEquals(1L, clock.getTime());
    }

    @Test
    @DisplayName("4. Test receive event correctly applies max(localClock, receivedTimestamp) + 1")
    public void testReceiveEventFormula() {
        LogicalClock clock = new LogicalClock("Node-Receiver", 5);
        long receivedTimestamp = 10;

        long newClock = clock.receiveEvent(receivedTimestamp);
        // max(5, 10) + 1 = 11
        assertEquals(11L, newClock);
        assertEquals(11L, clock.getTime());
    }

    @Test
    @DisplayName("5. Test receiving a larger timestamp advances the local clock")
    public void testReceivingLargerTimestamp() {
        LogicalClock clock = new LogicalClock("Node-1", 2);
        long receivedTimestamp = 100;

        long newTime = clock.receiveEvent(receivedTimestamp);
        assertEquals(101L, newTime);
        assertEquals(101L, clock.getTime());
    }

    @Test
    @DisplayName("6. Test receiving a smaller timestamp still increments the local clock")
    public void testReceivingSmallerTimestamp() {
        LogicalClock clock = new LogicalClock("Node-1", 10);
        long receivedTimestamp = 3;

        long newTime = clock.receiveEvent(receivedTimestamp);
        // max(10, 3) + 1 = 11
        assertEquals(11L, newTime);
        assertEquals(11L, clock.getTime());
    }

    @Test
    @DisplayName("7. Test independent nodes maintain independent clocks")
    public void testIndependentNodes() {
        LogicalClock nodeA = new LogicalClock("Node-A");
        LogicalClock nodeB = new LogicalClock("Node-B");

        nodeA.increment();
        nodeA.increment();
        nodeA.increment(); // nodeA = 3

        nodeB.increment(); // nodeB = 1

        assertEquals(3L, nodeA.getTime());
        assertEquals(1L, nodeB.getTime());
    }

    @Test
    @DisplayName("8. Test logical timestamps never decrease for events on the same node")
    public void testMonotonicity() {
        LogicalClock clock = new LogicalClock("Node-Monotonic");
        long prev = clock.getTime();

        for (int i = 0; i < 50; i++) {
            long current;
            if (i % 3 == 0) {
                current = clock.localEvent();
            } else if (i % 3 == 1) {
                current = clock.sendEvent();
            } else {
                current = clock.receiveEvent(i * 2);
            }
            assertTrue(current > prev, "Timestamp must strictly increase. Prev: " + prev + ", Current: " + current);
            prev = current;
        }
    }
}
