package com.chatreplay.replay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import com.chatreplay.event.EventType;
import com.chatreplay.event.SessionEvent;
import com.chatreplay.replay.SessionState.MessageStatus;
import com.chatreplay.session.Presence;
import com.chatreplay.session.SessionStatus;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

/** 리듀서 결정성: 같은 이벤트 열이면 언제/어떻게 적용해도 같은 상태. */
class SessionStateReducerTest {

    private static final UUID SID = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final Instant T0 = Instant.parse("2026-10-19T00:00:00Z");

    private final List<SessionEvent> log = new ArrayList<>();

    private SessionEvent ev(EventType type, String actor, Map<String, Object> payload) {
        long seq = log.size() + 1;
        SessionEvent e = new SessionEvent(SID, seq, UUID.nameUUIDFromBytes(("e" + seq).getBytes()), "c" + seq, type,
                actor, payload, null, T0.plusSeconds(seq));
        log.add(e);
        return e;
    }

    private void conversation() {
        ev(EventType.SESSION_CREATED, "alice", Map.of("title", "t"));
        ev(EventType.JOIN, "alice", Map.of("via", "ws"));          // 2
        ev(EventType.JOIN, "bob", Map.of("via", "rest"));          // 3
        ev(EventType.MESSAGE, "alice", Map.of("content", "hi"));   // 4
        ev(EventType.MESSAGE, "bob", Map.of("content", "hello"));  // 5
        ev(EventType.MESSAGE_EDITED, "alice", Map.of("targetSeq", 4, "content", "hi!")); // 6
        ev(EventType.DISCONNECT, "bob", Map.of("connectionId", "x")); // 7
        ev(EventType.MESSAGE, "alice", Map.of("content", "are you there?")); // 8
        ev(EventType.RECONNECT, "bob", Map.of("connectionId", "y")); // 9
        ev(EventType.READ, "bob", Map.of("upToSeq", 8));            // 10
        ev(EventType.MESSAGE_DELETED, "bob", Map.of("targetSeq", 5)); // 11
        ev(EventType.READ, "bob", Map.of("upToSeq", 4));            // 12: 늦게 온 오래된 READ → 되돌아가지 않음
        ev(EventType.LEAVE, "bob", Map.of());                       // 13
        ev(EventType.SESSION_ENDED, "alice", Map.of());             // 14
    }

    private SessionState replay(List<SessionEvent> events) {
        SessionState s = new SessionState(SID);
        SessionStateReducer.applyAll(s, events);
        return s;
    }

    @Test
    void replaysConversationIntoExpectedState() {
        conversation();
        SessionState s = replay(log);

        assertThat(s.seq()).isEqualTo(14);
        assertThat(s.status()).isEqualTo(SessionStatus.COMPLETED);
        assertThat(s.messages().get(4L).content()).isEqualTo("hi!");
        assertThat(s.messages().get(4L).status()).isEqualTo(MessageStatus.EDITED);
        assertThat(s.messages().get(5L).status()).isEqualTo(MessageStatus.DELETED);
        assertThat(s.messages().get(5L).content()).isNull();
        assertThat(s.participants().get("bob").lastReadSeq()).isEqualTo(8);
        assertThat(s.participants().get("bob").presence()).isEqualTo(Presence.OFFLINE);
    }

    @Test
    void statusFollowsPresenceAtEachPointInTime() {
        conversation();
        assertThat(replay(log.subList(0, 1)).status()).isEqualTo(SessionStatus.WAITING);
        assertThat(replay(log.subList(0, 3)).status()).isEqualTo(SessionStatus.ACTIVE);
        assertThat(replay(log.subList(0, 7)).status()).isEqualTo(SessionStatus.INTERRUPTED);
        assertThat(replay(log.subList(0, 9)).status()).isEqualTo(SessionStatus.ACTIVE);
    }

    @Test
    void duplicateAndOutOfOrderDeliveryIsIgnored() {
        conversation();
        SessionState expected = replay(log);

        // 같은 이벤트를 여러 번, 섞어서 넣어도 seq 기준으로 이미 반영된 것은 무시된다.
        List<SessionEvent> noisy = new ArrayList<>();
        for (SessionEvent e : log) {
            noisy.add(e);
            noisy.add(e);
            if (e.seq() > 3) {
                noisy.add(log.get((int) e.seq() - 3)); // 과거 이벤트 재등장
            }
        }
        SessionState actual = replay(noisy);
        assertThat(actual.toData()).isEqualTo(expected.toData());
    }

    @Test
    void gapInEventStreamFailsLoudly() {
        conversation();
        List<SessionEvent> withGap = new ArrayList<>(log);
        withGap.remove(5);
        assertThatThrownBy(() -> replay(withGap)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("gap");
    }

    @Test
    void snapshotPlusReplayEqualsFullReplayAtEverySeq() {
        conversation();
        JsonMapper json = JsonMapper.builder().build();
        for (int snapAt = 1; snapAt <= log.size(); snapAt++) {
            SessionState snap = replay(log.subList(0, snapAt));
            // 스냅샷은 JSON 으로 저장/로드된다
            SessionState.Data stored = json.readValue(json.writeValueAsString(snap.toData()), SessionState.Data.class);
            SessionState restored = SessionState.from(stored);
            SessionStateReducer.applyAll(restored, log.subList(snapAt, log.size()));
            assertThat(restored.toData()).as("snapshot at %d", snapAt).isEqualTo(replay(log).toData());
        }
    }

    @Test
    void editOrDeleteOfUnknownOrForeignMessageIsNoOp() {
        ev(EventType.SESSION_CREATED, "alice", Map.of());
        ev(EventType.JOIN, "alice", Map.of("via", "rest"));
        ev(EventType.JOIN, "bob", Map.of("via", "rest"));
        ev(EventType.MESSAGE, "alice", Map.of("content", "a"));
        ev(EventType.MESSAGE_EDITED, "alice", Map.of("targetSeq", 99, "content", "x")); // 없는 대상
        ev(EventType.MESSAGE_DELETED, "bob", Map.of("targetSeq", 4));                   // 남의 메시지
        SessionState s = replay(log);
        assertThat(s.messages()).hasSize(1);
        assertThat(s.messages().get(4L).status()).isEqualTo(MessageStatus.SENT);
    }

    @Test
    void randomizedConversationsAreDeterministic() {
        Random random = new Random(42);
        for (int round = 0; round < 50; round++) {
            log.clear();
            ev(EventType.SESSION_CREATED, "alice", Map.of());
            ev(EventType.JOIN, "alice", Map.of("via", "ws"));
            ev(EventType.JOIN, "bob", Map.of("via", "ws"));
            List<Long> messages = new ArrayList<>();
            for (int i = 0; i < 60; i++) {
                String actor = random.nextBoolean() ? "alice" : "bob";
                int op = random.nextInt(10);
                if (op < 5 || messages.isEmpty()) {
                    messages.add(ev(EventType.MESSAGE, actor, Map.of("content", "m" + i)).seq());
                } else if (op < 7) {
                    long target = messages.get(random.nextInt(messages.size()));
                    ev(EventType.MESSAGE_EDITED, actor, Map.of("targetSeq", target, "content", "e" + i));
                } else if (op < 8) {
                    long target = messages.get(random.nextInt(messages.size()));
                    ev(EventType.MESSAGE_DELETED, actor, Map.of("targetSeq", target));
                } else if (op < 9) {
                    ev(EventType.READ, actor, Map.of("upToSeq", random.nextInt(log.size() + 1)));
                } else {
                    ev(random.nextBoolean() ? EventType.DISCONNECT : EventType.RECONNECT, actor, Map.of());
                }
            }
            SessionState full = replay(log);
            int cut = 1 + random.nextInt(log.size() - 1);
            SessionState partial = replay(log.subList(0, cut));
            List<SessionEvent> rest = new ArrayList<>(log.subList(cut, log.size()));
            List<SessionEvent> shuffledDupes = new ArrayList<>(log.subList(0, cut));
            Collections.shuffle(shuffledDupes, random);
            rest.addAll(0, shuffledDupes); // 이미 반영된 이벤트가 무작위 순서로 다시 와도
            SessionStateReducer.applyAll(partial, rest);
            assertThat(partial.toData()).isEqualTo(full.toData());
        }
    }
}
