package com.chatreplay;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.chatreplay.support.IntegrationTest;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

/** 이벤트 수집: 멱등성, 순서(seq) 부여, 순서 뒤바뀜 처리, 검증 규칙. */
class EventIngestIntegrationTest extends IntegrationTest {

    @Test
    void resentEventIsStoredOnceAndReturnsOriginalSeq() {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        join(sid, "alice");

        JsonNode first = sendEvent(sid, "alice", "m-1", "MESSAGE", Map.of("content", "hello"));
        JsonNode retry = sendEvent(sid, "alice", "m-1", "MESSAGE", Map.of("content", "hello"));

        assertThat(first.get("status").asString()).isEqualTo("CREATED");
        assertThat(retry.get("status").asString()).isEqualTo("DUPLICATE");
        assertThat(retry.get("seq").asLong()).isEqualTo(first.get("seq").asLong());

        JsonNode events = get("/sessions/" + sid + "/events").body().get("events");
        long messages = 0;
        for (JsonNode e : events) {
            if (e.get("type").asString().equals("MESSAGE")) {
                messages++;
            }
        }
        assertThat(messages).isEqualTo(1);
    }

    @Test
    void sameKeyFromDifferentUsersAreDifferentEvents() {
        UUID sid = createSession("alice", List.of());
        join(sid, "alice");
        join(sid, "bob");
        long a = sendMessage(sid, "alice", "k", "from alice");
        long b = sendMessage(sid, "bob", "k", "from bob");
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void concurrentSendsGetGaplessUniqueSeqEvenWithRetries() throws Exception {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        join(sid, "alice");
        join(sid, "bob");
        long before = get("/sessions/" + sid).body().get("headSeq").asLong();

        int distinct = 40;
        ExecutorService pool = Executors.newFixedThreadPool(16);
        List<Future<JsonNode>> futures = new ArrayList<>();
        for (int i = 0; i < distinct; i++) {
            String user = i % 2 == 0 ? "alice" : "bob";
            String key = "c-" + i;
            // 각 메시지를 3번씩 동시에 보냄(네트워크 재전송 시뮬레이션)
            for (int r = 0; r < 3; r++) {
                futures.add(pool.submit(() -> sendEvent(sid, user, key, "MESSAGE", Map.of("content", key))));
            }
        }
        for (Future<JsonNode> f : futures) {
            assertThat(f.get().get("status").asString()).isIn("CREATED", "DUPLICATE");
        }
        pool.shutdown();

        JsonNode page = get("/sessions/" + sid + "/events?afterSeq=" + before + "&limit=1000").body();
        JsonNode events = page.get("events");
        assertThat(events.size()).isEqualTo(distinct);
        for (int i = 0; i < events.size(); i++) {
            assertThat(events.get(i).get("seq").asLong()).isEqualTo(before + 1 + i); // 빈틈 없는 단조 증가
        }
        // server_ts 도 seq 순서와 같은 방향(비감소)
        for (int i = 1; i < events.size(); i++) {
            String prev = events.get(i - 1).get("serverTs").asString();
            String cur = events.get(i).get("serverTs").asString();
            assertThat(java.time.Instant.parse(cur)).isAfterOrEqualTo(java.time.Instant.parse(prev));
        }
    }

    @Test
    void editArrivingBeforeOriginalIsRejectedAsRetryableThenSucceeds() {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        join(sid, "alice");
        long head = get("/sessions/" + sid).body().get("headSeq").asLong();
        long expectedTarget = head + 1;

        // 순서 뒤바뀜: 수정 이벤트가 원본보다 먼저 도착
        JsonNode early = sendEvent(sid, "alice", "edit-1", "MESSAGE_EDITED",
                Map.of("targetSeq", expectedTarget, "content", "edited"));
        assertThat(early.get("status").asString()).isEqualTo("REJECTED");
        assertThat(early.get("code").asString()).isEqualTo("TARGET_NOT_FOUND");
        assertThat(early.get("retryable").asBoolean()).isTrue();

        long target = sendMessage(sid, "alice", "orig-1", "original");
        assertThat(target).isEqualTo(expectedTarget);

        // 클라이언트가 같은 키로 재시도 → 정상 반영
        JsonNode retried = sendEvent(sid, "alice", "edit-1", "MESSAGE_EDITED",
                Map.of("targetSeq", target, "content", "edited"));
        assertThat(retried.get("status").asString()).isEqualTo("CREATED");

        JsonNode msg = get("/sessions/" + sid + "/timeline").body().get("messages").get(0);
        assertThat(msg.get("content").asString()).isEqualTo("edited");
        assertThat(msg.get("status").asString()).isEqualTo("EDITED");
    }

    @Test
    void editOfNonMessageSeqIsNotRetryable() {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        join(sid, "alice");
        // seq 1 은 SESSION_CREATED: 이미 지나간 seq 라 기다려도 메시지가 되지 않는다
        JsonNode r = sendEvent(sid, "alice", "edit-created", "MESSAGE_EDITED", Map.of("targetSeq", 1, "content", "x"));
        assertThat(r.get("code").asString()).isEqualTo("TARGET_NOT_A_MESSAGE");
        assertThat(r.get("retryable").asBoolean()).isFalse();
    }

    @Test
    void validationErrorsAreReadable() {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        Res r = post("/sessions/" + sid + "/events", "alice", Map.of("events", List.of(
                Map.of("type", "MESSAGE", "payload", Map.of("content", "x")))));   // clientEventId 누락
        assertThat(r.status()).isEqualTo(400);
        assertThat(r.body().get("detail").asString()).doesNotContain("public ");
        assertThat(r.body().get("errors").get(0).get("field").asString()).contains("clientEventId");
    }

    @Test
    void rejectsNegativeReadPosition() {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        join(sid, "alice");
        JsonNode r = sendEvent(sid, "alice", "read-neg", "READ", Map.of("upToSeq", -5));
        assertThat(r.get("status").asString()).isEqualTo("REJECTED");
        assertThat(r.get("code").asString()).isEqualTo("INVALID_REQUEST");
    }

    @Test
    void batchReportsPerItemResults() {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        join(sid, "alice");
        Res r = post("/sessions/" + sid + "/events", "alice", Map.of("events", List.of(
                Map.of("clientEventId", "b1", "type", "MESSAGE", "payload", Map.of("content", "one")),
                Map.of("clientEventId", "b2", "type", "MESSAGE", "payload", Map.of()),           // content 없음
                Map.of("clientEventId", "b3", "type", "DISCONNECT", "payload", Map.of()),        // 서버 전용 타입
                Map.of("clientEventId", "b1", "type", "MESSAGE", "payload", Map.of("content", "one")))));
        JsonNode results = r.body().get("results");
        assertThat(results.get(0).get("status").asString()).isEqualTo("CREATED");
        assertThat(results.get(1).get("status").asString()).isEqualTo("REJECTED");
        assertThat(results.get(2).get("code").asString()).isEqualTo("EVENT_TYPE_NOT_ALLOWED");
        assertThat(results.get(3).get("status").asString()).isEqualTo("DUPLICATE");
    }

    @Test
    void enforcesOneToOneAndParticipation() {
        UUID sid = createSession("alice", List.of());
        join(sid, "alice");
        join(sid, "bob");
        assertThat(post("/sessions/" + sid + "/join", "carol", null).status()).isEqualTo(409);

        UUID invited = createSession("alice", List.of("alice", "bob"));
        assertThat(post("/sessions/" + invited + "/join", "mallory", null).status()).isEqualTo(403);

        JsonNode notJoined = sendEvent(invited, "bob", "x", "MESSAGE", Map.of("content", "hi"));
        assertThat(notJoined.get("code").asString()).isEqualTo("NOT_A_PARTICIPANT");

        // 재참여 요청은 이벤트 없이 SKIPPED (상태 기반 멱등)
        Res again = post("/sessions/" + sid + "/join", "alice", null);
        assertThat(again.body().get("outcome").asString()).isEqualTo("SKIPPED");
    }

    @Test
    void endIsIdempotentAndBlocksFurtherMessages() {
        UUID sid = createSession("alice", List.of("alice", "bob"));
        join(sid, "alice");
        Res first = post("/sessions/" + sid + "/end", "alice", null);
        Res second = post("/sessions/" + sid + "/end", "alice", null);
        assertThat(first.body().get("outcome").asString()).isEqualTo("APPENDED");
        assertThat(second.body().get("outcome").asString()).isEqualTo("DUPLICATE");
        assertThat(first.body().get("session").get("status").asString()).isEqualTo("COMPLETED");

        JsonNode after = sendEvent(sid, "alice", "late", "MESSAGE", Map.of("content", "late"));
        assertThat(after.get("code").asString()).isEqualTo("SESSION_ENDED");
    }

    @Test
    void listFiltersByStatusAndParticipantWithCursor() {
        String user = "u-" + UUID.randomUUID().toString().substring(0, 8);
        List<UUID> created = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            UUID sid = createSession(user, List.of());
            join(sid, user);
            created.add(sid);
        }
        post("/sessions/" + created.get(0) + "/end", user, null);

        JsonNode page1 = get("/sessions?participant=" + user + "&limit=2").body();
        assertThat(page1.get("items").size()).isEqualTo(2);
        String cursor = page1.get("nextCursor").asString();
        JsonNode page2 = get("/sessions?participant=" + user + "&limit=10&cursor=" + cursor).body();
        assertThat(page2.get("items").size()).isEqualTo(3);
        assertThat(page2.get("nextCursor").isNull()).isTrue();

        JsonNode completed = get("/sessions?participant=" + user + "&status=COMPLETED").body();
        assertThat(completed.get("items").size()).isEqualTo(1);
        assertThat(completed.get("items").get(0).get("id").asString()).isEqualTo(created.get(0).toString());
    }
}
