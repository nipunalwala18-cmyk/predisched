package com.predisched.dashboard.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.dashboard.DashboardProperties;
import com.predisched.dashboard.cluster.Cluster;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

/** The stream batches per topic: at most 4-5 messages a second whatever the event rate. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"spring.datasource.url=jdbc:postgresql://localhost:1/none",
                "spring.datasource.hikari.initialization-fail-timeout=-1"})
class StreamTest {

    @LocalServerPort
    int port;

    @MockBean
    Cluster cluster;

    @Autowired
    StreamBridge bridge;

    @Autowired
    DashboardProperties props;

    @Test
    void throttlerSendsOneBatchPerTopicPerTick() {
        List<String> sent = new ArrayList<>();
        List<Integer> sizes = new ArrayList<>();
        Throttler t = new Throttler((topic, batch) -> {
            sent.add(topic);
            sizes.add(batch.size());
        }, 3);
        for (int i = 0; i < 5; i++) {
            t.add("events", i);
        }
        t.add("tasks", "x");
        t.flush();
        t.flush();   // nothing new: nothing sent
        assertEquals(List.of("events", "tasks"), sent);
        assertEquals(List.of(3, 1), sizes, "the newest 3 kept");
        assertEquals(2, t.dropped());
    }

    @Test
    void aStompClientReceivesThrottledBatches() throws Exception {
        WebSocketStompClient client = new WebSocketStompClient(new StandardWebSocketClient());
        client.setMessageConverter(new MappingJackson2MessageConverter());
        StompSession session = client.connectAsync("ws://localhost:" + port + "/ws/stream",
                new StompSessionHandlerAdapter() {}).get(10, TimeUnit.SECONDS);
        List<Long> arrivals = new CopyOnWriteArrayList<>();
        List<Integer> sizes = new CopyOnWriteArrayList<>();
        session.subscribe("/topic/events", new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders headers) {
                return List.class;
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
                arrivals.add(System.nanoTime());
                sizes.add(((List<?>) payload).size());
            }
        });
        Thread.sleep(300);
        // 400 events over 2 s: 200 a second, far above the stream rate.
        for (int i = 0; i < 400; i++) {
            bridge.publish("events", Map.of("type", "TEST", "n", i));
            Thread.sleep(5);
        }
        Thread.sleep(800);
        session.disconnect();
        assertEquals(400, sizes.stream().mapToInt(Integer::intValue).sum(), "nothing lost");
        long first = arrivals.get(0);
        int[] perSecond = new int[10];
        for (long a : arrivals) {
            perSecond[(int) ((a - first) / 1_000_000_000L)]++;
        }
        for (int n : perSecond) {
            assertTrue(n <= 5, "messages in one second: " + n);
        }
        assertTrue(props.getStreamRatePerSecond() == 4);
    }
}
