package com.chatreplay;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.chatreplay.projection.ProjectionFaultInjector;
import com.chatreplay.projection.ProjectionWorker;
import com.chatreplay.support.IntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import tools.jackson.databind.JsonNode;

/** 특정 시점 복원(스냅샷 vs 전체 리플레이), 비동기 프로젝션, 재시도/DLQ. */
class TimelineAndProjectionIntegrationTest extends IntegrationTest {

    @Autowired
    ProjectionWorker worker;

    @Autowired
    ProjectionFaultInjector faults;

    @AfterEach
    void heal() {
        faults.healAll();
    }

    private void drainWorker() {
        for (int i = 0; i < 20 && worker.runOnce() > 0; i++) {
            // outbox 가 빌 때까지
        }
    }

    @Test
    void restoresStateAtPointInTime() throws Exception {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        join(sid, "alice");
        join(sid, "bob");
        long m1 = sendMessage(sid, "alice", "a1", "first");
        Thread.sleep(5);
        Instant between = Instant.parse(get("/sessions/" + sid).body().get("lastEventAt").asString());
        Thread.sleep(5);
        sendEvent(sid, "alice", "e1", "MESSAGE_EDITED", Map.of("targetSeq", m1, "content", "first (edited)"));
        long m2 = sendMessage(sid, "bob", "b1", "second");
        sendEvent(sid, "bob", "d1", "MESSAGE_DELETED", Map.of("targetSeq", m2));
        post("/sessions/" + sid + "/leave", "bob", null);

        // 과거 시점: bob 참여 중, 메시지 1개, 수정 전
        JsonNode past = get("/sessions/" + sid + "/timeline?at=" + between).body();
        assertThat(past.get("seq").asLong()).isEqualTo(m1);
        assertThat(past.get("totalMessages").asInt()).isEqualTo(1);
        assertThat(past.get("messages").get(0).get("content").asString()).isEqualTo("first");
        assertThat(past.get("participants").size()).isEqualTo(2);
        assertThat(past.get("participants").get(1).get("state").asString()).isEqualTo("JOINED");
        // bob 은 alice 의 메시지 1개를 안 읽음
        assertThat(past.get("participants").get(1).get("unreadCount").asInt()).isEqualTo(1);

        // 현재: 수정/삭제/퇴장 반영
        JsonNode now = get("/sessions/" + sid + "/timeline").body();
        assertThat(now.get("messages").get(0).get("content").asString()).isEqualTo("first (edited)");
        assertThat(now.get("messages").get(1).get("status").asString()).isEqualTo("DELETED");
        assertThat(now.get("participants").get(1).get("state").asString()).isEqualTo("LEFT");

        // 세션 생성 이전 시각은 404
        assertThat(get("/sessions/" + sid + "/timeline?at=2000-01-01T00:00:00Z").status()).isEqualTo(404);
    }

    @Test
    void snapshotReplayMatchesFullReplayAtEverySeq() {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        join(sid, "alice");
        join(sid, "bob");
        for (int i = 0; i < 23; i++) {
            long seq = sendMessage(sid, i % 2 == 0 ? "alice" : "bob", "m" + i, "msg " + i);
            if (i % 4 == 3) {
                sendEvent(sid, i % 2 == 0 ? "alice" : "bob", "e" + i, "MESSAGE_EDITED",
                        Map.of("targetSeq", seq, "content", "edited " + i));
            }
        }
        drainWorker(); // interval=5 → 자동 스냅샷 생성

        JsonNode snaps = get("/sessions/" + sid + "/snapshots").body();
        assertThat(snaps.size()).isGreaterThanOrEqualTo(4);

        long head = get("/sessions/" + sid).body().get("headSeq").asLong();
        for (long seq = 1; seq <= head; seq++) {
            JsonNode auto = get("/sessions/" + sid + "/timeline?atSeq=" + seq + "&messageLimit=500").body();
            JsonNode full = get("/sessions/" + sid + "/timeline?atSeq=" + seq + "&messageLimit=500&mode=FULL").body();
            assertThat(full.get("restore").get("strategy").asString()).isEqualTo("FULL_REPLAY");
            assertThat(withoutRestoreInfo(auto)).as("seq %d", seq).isEqualTo(withoutRestoreInfo(full));
        }
        JsonNode atHead = get("/sessions/" + sid + "/timeline").body();
        assertThat(atHead.get("restore").get("strategy").asString()).isEqualTo("SNAPSHOT_REPLAY");
        assertThat(atHead.get("restore").get("replayedEvents").asInt()).isLessThan(5);
    }

    private JsonNode withoutRestoreInfo(JsonNode node) {
        var copy = node.deepCopy();
        ((tools.jackson.databind.node.ObjectNode) copy).remove("restore");
        return copy;
    }

    @Test
    void projectionCatchesUpAsynchronously() {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        join(sid, "alice");
        sendMessage(sid, "alice", "p1", "hello");
        long m2 = sendMessage(sid, "alice", "p2", "world");

        JsonNode before = get("/sessions/" + sid).body().get("summary");
        assertThat(before.get("lag").asLong()).isGreaterThan(0); // 아직 반영 전(최종적 일관성)

        drainWorker();
        JsonNode after = get("/sessions/" + sid).body().get("summary");
        assertThat(after.get("lag").asLong()).isZero();
        assertThat(after.get("messageCount").asInt()).isEqualTo(2);
        assertThat(after.get("lastMessageSeq").asLong()).isEqualTo(m2);
        assertThat(after.get("lastMessagePreview").asString()).isEqualTo("world");

        // 워커가 같은 작업을 다시 실행해도(no-op) 결과가 같다
        worker.runOnce();
        assertThat(get("/sessions/" + sid).body().get("summary").get("messageCount").asInt()).isEqualTo(2);
    }

    @Test
    void failingProjectionIsRetriedThenMovedToDlqAndRedriven() throws Exception {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        join(sid, "alice");
        sendMessage(sid, "alice", "x1", "hello");
        faults.failSession(sid);

        // max-attempts=3, 백오프 1~2ms
        for (int i = 0; i < 10; i++) {
            worker.runOnce();
            Thread.sleep(5);
        }
        JsonNode dlq = get("/admin/projections/dlq").body();
        JsonNode entry = null;
        for (JsonNode e : dlq) {
            if (e.get("sessionId").asString().equals(sid.toString())) {
                entry = e;
            }
        }
        assertThat(entry).isNotNull();
        assertThat(entry.get("attempts").asInt()).isEqualTo(3);
        assertThat(entry.get("lastError").asString()).contains("injected");
        assertThat(get("/sessions/" + sid).body().get("summary").get("messageCount").asInt()).isZero();

        // 원인 해결 → 재처리
        faults.heal(sid);
        post("/admin/projections/dlq/" + entry.get("id").asLong() + "/redrive", null, null);
        drainWorker();
        assertThat(get("/sessions/" + sid).body().get("summary").get("messageCount").asInt()).isEqualTo(1);
    }

    @Test
    void rebuildProducesSameProjection() {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        join(sid, "alice");
        sendMessage(sid, "alice", "r1", "a");
        long m = sendMessage(sid, "alice", "r2", "b");
        sendEvent(sid, "alice", "r3", "MESSAGE_DELETED", Map.of("targetSeq", m));
        drainWorker();
        JsonNode original = get("/sessions/" + sid).body().get("summary");

        post("/admin/sessions/" + sid + "/projection/rebuild", null, null);
        drainWorker();
        assertThat(get("/sessions/" + sid).body().get("summary")).isEqualTo(original);
    }
}
