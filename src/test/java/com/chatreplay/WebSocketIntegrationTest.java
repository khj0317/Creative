package com.chatreplay;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import com.chatreplay.realtime.InstanceHeartbeat;
import com.chatreplay.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import tools.jackson.databind.JsonNode;

/** 실시간 송수신, presence, 재연결 resume, 장애 인스턴스 정리. */
class WebSocketIntegrationTest extends IntegrationTest {

    @Autowired
    InstanceHeartbeat heartbeat;

    @Autowired
    JdbcClient jdbc;

    /** 테스트용 WebSocket 클라이언트: 받은 frame 을 큐에 쌓는다. */
    class Client implements WebSocket.Listener {
        final BlockingQueue<JsonNode> frames = new LinkedBlockingQueue<>();
        final StringBuilder buffer = new StringBuilder();
        final java.util.concurrent.CompletableFuture<Integer> closed = new java.util.concurrent.CompletableFuture<>();
        WebSocket ws;

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            closed.complete(statusCode);
            return null;
        }

        Client connect(UUID sid, String user, Long lastSeq) throws Exception {
            ws = http.newWebSocketBuilder().buildAsync(wsUri(sid, user, lastSeq), this).get(5, TimeUnit.SECONDS);
            return this;
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            buffer.append(data);
            if (last) {
                frames.add(json.readTree(buffer.toString()));
                buffer.setLength(0);
            }
            webSocket.request(1);
            return null;
        }

        void send(Map<String, Object> frame) {
            ws.sendText(json.writeValueAsString(frame), true).join();
        }

        JsonNode await(Predicate<JsonNode> match) throws InterruptedException {
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            while (System.nanoTime() < deadline) {
                JsonNode f = frames.poll(100, TimeUnit.MILLISECONDS);
                if (f != null && match.test(f)) {
                    return f;
                }
            }
            throw new AssertionError("frame not received");
        }

        JsonNode awaitEvent(String type) throws InterruptedException {
            return await(f -> f.get("type").asString().equals("event")
                    && f.get("event").get("type").asString().equals(type)).get("event");
        }

        void close() {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "bye").join();
        }

        void abort() {
            ws.abort();
        }

        int awaitClose() throws Exception {
            return closed.get(5, TimeUnit.SECONDS);
        }
    }

    private JsonNode session(UUID sid) {
        return get("/sessions/" + sid).body();
    }

    private String presenceOf(UUID sid, String user) {
        for (JsonNode p : session(sid).get("participants")) {
            if (p.get("userId").asString().equals(user)) {
                return p.get("presence").asString();
            }
        }
        return null;
    }

    private void awaitCondition(java.util.function.BooleanSupplier cond) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!cond.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met");
            }
            Thread.sleep(50);
        }
    }

    @Test
    void messagesAreDeliveredInRealtimeWithAck() throws Exception {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        Client alice = new Client().connect(sid, "alice", null);
        JsonNode welcome = alice.await(f -> f.get("type").asString().equals("welcome"));
        assertThat(welcome.get("connectionId").asString()).isNotBlank();

        Client bob = new Client().connect(sid, "bob", null);
        alice.awaitEvent("JOIN"); // bob 입장 알림
        assertThat(session(sid).get("status").asString()).isEqualTo("ACTIVE");
        assertThat(presenceOf(sid, "alice")).isEqualTo("ONLINE");

        alice.send(Map.of("type", "send", "clientEventId", "ws-1", "eventType", "MESSAGE",
                "payload", Map.of("content", "hi bob")));
        JsonNode ack = alice.await(f -> f.get("type").asString().equals("ack"));
        assertThat(ack.get("outcome").asString()).isEqualTo("APPENDED");

        JsonNode received = bob.awaitEvent("MESSAGE");
        assertThat(received.get("payload").get("content").asString()).isEqualTo("hi bob");
        assertThat(received.get("seq").asLong()).isEqualTo(ack.get("seq").asLong());

        // 같은 clientEventId 재전송 → DUPLICATE ack, 상대에게 다시 전달되지 않음
        alice.send(Map.of("type", "send", "clientEventId", "ws-1", "eventType", "MESSAGE",
                "payload", Map.of("content", "hi bob")));
        JsonNode dup = alice.await(f -> f.get("type").asString().equals("ack"));
        assertThat(dup.get("outcome").asString()).isEqualTo("DUPLICATE");
        assertThat(dup.get("seq").asLong()).isEqualTo(ack.get("seq").asLong());

        // typing 은 저장하지 않고 상대에게만 전달
        alice.send(Map.of("type", "typing", "isTyping", true));
        JsonNode typing = bob.await(f -> f.get("type").asString().equals("typing"));
        assertThat(typing.get("userId").asString()).isEqualTo("alice");

        alice.close();
        bob.close();
    }

    @Test
    void disconnectThenReconnectResumesMissedEventsInOrder() throws Exception {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        Client alice = new Client().connect(sid, "alice", null);
        alice.await(f -> f.get("type").asString().equals("welcome"));
        Client bob = new Client().connect(sid, "bob", null);
        bob.await(f -> f.get("type").asString().equals("welcome"));

        alice.send(Map.of("type", "send", "clientEventId", "r-1", "payload", Map.of("content", "before")));
        long lastSeen = bob.awaitEvent("MESSAGE").get("seq").asLong();

        // bob 의 네트워크가 끊김(비정상 종료)
        bob.abort();
        alice.awaitEvent("DISCONNECT");
        awaitCondition(() -> "DISCONNECTED".equals(presenceOf(sid, "bob")));
        assertThat(session(sid).get("status").asString()).isEqualTo("INTERRUPTED");
        // 쓰기 모델(sessions.status)과 이벤트 리플레이 결과가 같은 규칙으로 일치
        assertThat(get("/sessions/" + sid + "/timeline").body().get("status").asString()).isEqualTo("INTERRUPTED");

        // 끊긴 동안 alice 가 보낸 메시지
        List<Long> missed = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            alice.send(Map.of("type", "send", "clientEventId", "r-miss-" + i,
                    "payload", Map.of("content", "missed " + i)));
            missed.add(alice.await(f -> f.get("type").asString().equals("ack")).get("seq").asLong());
        }

        // bob 이 마지막으로 받은 seq 로 재연결 → 놓친 이벤트를 순서대로 받고 RECONNECT 가 기록된다
        Client bob2 = new Client().connect(sid, "bob", lastSeen);
        List<Long> resumed = new ArrayList<>();
        long prev = lastSeen;
        while (resumed.size() < missed.size()) {
            JsonNode e = bob2.await(f -> f.get("type").asString().equals("event")).get("event");
            assertThat(e.get("seq").asLong()).isEqualTo(prev + 1); // 빈틈/중복 없이 연속
            prev = e.get("seq").asLong();
            if (e.get("type").asString().equals("MESSAGE")) {
                resumed.add(e.get("seq").asLong());
            }
        }
        assertThat(resumed).containsExactlyElementsOf(missed);
        alice.awaitEvent("RECONNECT");
        assertThat(session(sid).get("status").asString()).isEqualTo("ACTIVE");
        assertThat(presenceOf(sid, "bob")).isEqualTo("ONLINE");
        JsonNode replayed = get("/sessions/" + sid + "/timeline").body();
        assertThat(replayed.get("status").asString()).isEqualTo("ACTIVE");
        assertThat(replayed.get("participants").get(1).get("presence").asString()).isEqualTo("ONLINE");

        alice.close();
        bob2.close();
    }

    @Test
    void newConnectionReplacesOldOneWithoutDisconnectEvent() throws Exception {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        Client first = new Client().connect(sid, "alice", null);
        first.await(f -> f.get("type").asString().equals("welcome"));
        Client second = new Client().connect(sid, "alice", null);
        second.await(f -> f.get("type").asString().equals("welcome"));

        Thread.sleep(300);
        JsonNode events = get("/sessions/" + sid + "/events").body().get("events");
        for (JsonNode e : events) {
            assertThat(e.get("type").asString()).isNotEqualTo("DISCONNECT");
        }
        assertThat(presenceOf(sid, "alice")).isEqualTo("ONLINE");
        second.close();
    }

    @Test
    void reaperDisconnectsConnectionsOfDeadInstances() throws Exception {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        join(sid, "bob");
        // 다른 인스턴스(이미 죽어서 하트비트가 없음)에 bob 의 연결이 붙어 있던 상황을 만든다
        jdbc.sql("""
                        UPDATE session_participants SET presence = 'ONLINE', connection_id = 'conn-dead',
                               connected_instance = 'dead-instance'
                         WHERE session_id = :sid AND user_id = 'bob'
                        """)
                .param("sid", sid)
                .update();

        assertThat(heartbeat.reapOnce()).isGreaterThanOrEqualTo(1);
        assertThat(presenceOf(sid, "bob")).isEqualTo("DISCONNECTED");
        assertThat(heartbeat.reapOnce()).isZero(); // 두 번 실행해도 한 번만 기록(멱등)

        JsonNode events = get("/sessions/" + sid + "/events").body().get("events");
        JsonNode last = events.get(events.size() - 1);
        assertThat(last.get("type").asString()).isEqualTo("DISCONNECT");
        assertThat(last.get("payload").get("reason").asString()).isEqualTo("INSTANCE_DOWN");
        assertThat(last.get("clientEventId").asString()).isEqualTo("disconnect:conn-dead");
    }

    @Test
    void leaveClosesConnectionsSoLeftUserStopsReceiving() throws Exception {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        Client alice = new Client().connect(sid, "alice", null);
        alice.await(f -> f.get("type").asString().equals("welcome"));
        Client bob = new Client().connect(sid, "bob", null);
        bob.await(f -> f.get("type").asString().equals("welcome"));
        Client bobTab2 = new Client().connect(sid, "bob", null); // 같은 사용자의 두 번째 연결(첫 연결은 인계로 닫힘)
        bobTab2.await(f -> f.get("type").asString().equals("welcome"));

        bobTab2.send(Map.of("type", "send", "clientEventId", "bye", "eventType", "LEAVE", "payload", Map.of()));
        JsonNode ack = bobTab2.await(f -> f.get("type").asString().equals("ack"));
        assertThat(ack.get("outcome").asString()).isEqualTo("APPENDED");
        assertThat(bobTab2.awaitClose()).isEqualTo(1000);   // 퇴장한 연결은 ack 후 닫힌다
        alice.awaitEvent("LEAVE");

        // 퇴장은 연결 끊김이 아니므로 DISCONNECT 가 남지 않고, 세션은 중단 상태가 되지 않는다
        Thread.sleep(200);
        for (JsonNode e : get("/sessions/" + sid + "/events").body().get("events")) {
            assertThat(e.get("type").asString()).isNotEqualTo("DISCONNECT");
        }
        assertThat(session(sid).get("status").asString()).isEqualTo("ACTIVE");
        alice.close();
    }

    @Test
    void restLeaveClosesOpenWebSocketOfThatUser() throws Exception {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        Client bob = new Client().connect(sid, "bob", null);
        bob.await(f -> f.get("type").asString().equals("welcome"));

        assertThat(post("/sessions/" + sid + "/leave", "bob", null).status()).isEqualTo(200);
        assertThat(bob.awaitClose()).isEqualTo(4001);
        assertThat(presenceOf(sid, "bob")).isEqualTo("OFFLINE");
    }

    @Test
    void lastSeqBeyondHeadIsClampedSoClientStillReceivesEvents() throws Exception {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        Client alice = new Client().connect(sid, "alice", null);
        alice.await(f -> f.get("type").asString().equals("welcome"));
        // 잘못된(미래) lastSeq 로 접속해도 이후 이벤트를 받아야 한다
        Client bob = new Client().connect(sid, "bob", 9999L);
        JsonNode welcome = bob.await(f -> f.get("type").asString().equals("welcome"));
        assertThat(welcome.get("resumeFromSeq").asLong()).isEqualTo(welcome.get("headSeq").asLong());

        alice.send(Map.of("type", "send", "clientEventId", "after-stale", "payload", Map.of("content", "hello")));
        assertThat(bob.awaitEvent("MESSAGE").get("payload").get("content").asString()).isEqualTo("hello");

        // resume 프레임도 같은 규칙
        bob.send(Map.of("type", "resume", "lastSeq", 9999));
        Thread.sleep(200);
        alice.send(Map.of("type", "send", "clientEventId", "after-resume", "payload", Map.of("content", "again")));
        assertThat(bob.awaitEvent("MESSAGE").get("payload").get("content").asString()).isEqualTo("again");
        alice.close();
        bob.close();
    }

    @Test
    void rejectsUninvitedUser() throws Exception {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        Client mallory = new Client().connect(sid, "mallory", null);
        JsonNode err = mallory.await(f -> f.get("type").asString().equals("error"));
        assertThat(err.get("code").asString()).isEqualTo("NOT_INVITED");
    }
}
