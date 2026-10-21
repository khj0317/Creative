package com.chatreplay.event;

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.chatreplay.common.ApiException;
import com.chatreplay.common.ErrorCode;
import com.chatreplay.projection.ProjectionOutboxRepository;
import com.chatreplay.session.ParticipantRepository;
import com.chatreplay.session.ParticipantRow;
import com.chatreplay.session.ParticipantState;
import com.chatreplay.session.Presence;
import com.chatreplay.session.SessionRepository;
import com.chatreplay.session.SessionRow;
import com.chatreplay.session.SessionStatus;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

/**
 * 이벤트 추가의 유일한 쓰기 경로. 한 트랜잭션 안에서 다음을 수행한다.
 * <ol>
 *   <li>세션 행 잠금(FOR UPDATE)으로 세션 단위 쓰기 직렬화</li>
 *   <li>멱등 키 조회 → 이미 있으면 기존 이벤트를 그대로 반환(중복 저장/중복 반영 없음)</li>
 *   <li>명령 검증(참여 여부, 정원, 수정/삭제 대상 존재 등)</li>
 *   <li>seq = last_seq + 1, server_ts = max(now, last_event_at) 부여 후 이벤트 저장</li>
 *   <li>쓰기 모델(참여자/세션 상태) 갱신 + projection_outbox 적재(Transactional Outbox)</li>
 * </ol>
 * 이벤트 저장과 outbox 적재가 같은 트랜잭션이므로 "이벤트는 있는데 프로젝션 신호가 유실" 되는 부분 실패가 없다.
 */
@Component
public class EventAppender {

    public static final int MAX_PARTICIPANTS = 2;
    public static final int MAX_CONTENT_LENGTH = 4000;

    private final SessionRepository sessions;
    private final ParticipantRepository participants;
    private final EventRepository events;
    private final ProjectionOutboxRepository outbox;
    private final Clock clock;

    public EventAppender(SessionRepository sessions, ParticipantRepository participants, EventRepository events,
                         ProjectionOutboxRepository outbox, Clock clock) {
        this.sessions = sessions;
        this.participants = participants;
        this.events = events;
        this.outbox = outbox;
        this.clock = clock;
    }

    @Transactional
    public AppendResult append(AppendCommand cmd) {
        if (!StringUtils.hasText(cmd.clientEventId()) || cmd.clientEventId().length() > 80) {
            throw new ApiException(ErrorCode.INVALID_REQUEST, "clientEventId 는 1~80자여야 합니다.");
        }
        SessionRow session = sessions.lockById(cmd.sessionId())
                .orElseThrow(() -> new ApiException(ErrorCode.SESSION_NOT_FOUND, "세션이 없습니다: " + cmd.sessionId()));

        // 1) 멱등성: 같은 키의 이벤트가 있으면 저장/반영 없이 기존 결과를 돌려준다.
        var existing = events.findByIdempotencyKey(cmd.sessionId(), cmd.actorId(), cmd.clientEventId());
        if (existing.isPresent()) {
            return AppendResult.duplicate(existing.get(), session.lastSeq(), session.status());
        }
        if (session.ended()) {
            if (cmd.type() == EventType.DISCONNECT) {
                return AppendResult.skipped(session.lastSeq(), session.status(), "session ended");
            }
            throw new ApiException(ErrorCode.SESSION_ENDED, "이미 종료된 세션입니다.");
        }

        Map<String, ParticipantRow> roster = new LinkedHashMap<>();
        for (ParticipantRow p : participants.findBySession(cmd.sessionId())) {
            roster.put(p.userId(), p);
        }

        Instant now = clock.instant();
        // server_ts 비감소 보장: 인스턴스 간 시계 오차가 있어도 seq 순서와 시각 순서가 뒤집히지 않는다.
        Instant serverTs = now.isAfter(session.lastEventAt()) ? now : session.lastEventAt();

        Decision d = decide(cmd, session, roster, serverTs);
        if (d.eventType() == null) {
            if (d.participant() != null) {
                participants.upsert(d.participant());
            }
            return AppendResult.skipped(session.lastSeq(), session.status(), d.skipReason());
        }

        long seq = session.lastSeq() + 1;
        SessionEvent event = new SessionEvent(cmd.sessionId(), seq, UUID.randomUUID(), cmd.clientEventId(),
                d.eventType(), cmd.actorId(), d.payload(), cmd.clientTs(), serverTs);
        if (d.participant() != null) {
            participants.upsert(d.participant());
            roster.put(d.participant().userId(), d.participant());
        }
        boolean ended = d.eventType() == EventType.SESSION_ENDED;
        SessionStatus status = SessionStatus.derive(ended,
                roster.values().stream().map(ParticipantRow::status).toList());
        // 이벤트 INSERT + 세션 UPDATE + outbox UPSERT 를 한 문장(writable CTE)으로: DB 왕복 3 → 1
        events.appendAndAdvance(event, status, ended ? serverTs : null, now);
        return AppendResult.appended(event, status);
    }

    /** eventType 이 null 이면 이벤트를 남기지 않는다(participant 갱신만 있을 수 있음). */
    private record Decision(EventType eventType, Map<String, Object> payload, ParticipantRow participant,
                            String skipReason) {

        static Decision event(EventType type, Map<String, Object> payload, ParticipantRow participant) {
            return new Decision(type, payload, participant, null);
        }

        static Decision skip(String reason, ParticipantRow participant) {
            return new Decision(null, Map.of(), participant, reason);
        }
    }

    private Decision decide(AppendCommand cmd, SessionRow session, Map<String, ParticipantRow> roster, Instant ts) {
        ParticipantRow actor = roster.get(cmd.actorId());
        Map<String, Object> in = cmd.payload() == null ? Map.of() : cmd.payload();
        return switch (cmd.type()) {
            case JOIN -> decideJoin(cmd, session, roster, actor, ts);
            case RECONNECT -> decideConnect(cmd, session, roster, actor, ts);
            case LEAVE -> {
                requireJoined(actor);
                yield Decision.event(EventType.LEAVE, Map.of(), new ParticipantRow(cmd.sessionId(), actor.userId(),
                        ParticipantState.LEFT, Presence.OFFLINE, actor.joinedAt(), ts, ts, null, null));
            }
            case DISCONNECT -> {
                // 이 연결이 아직 참여자의 "현재 연결" 일 때만 기록한다.
                // 새 연결이 이미 붙었거나(재연결/다른 인스턴스로 이동) 퇴장했다면 늦게 도착한 종료 신호이므로 무시.
                if (actor == null || !actor.joined() || cmd.connectionId() == null
                        || !cmd.connectionId().equals(actor.connectionId())) {
                    yield Decision.skip("stale connection", null);
                }
                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("connectionId", cmd.connectionId());
                payload.put("reason", String.valueOf(in.getOrDefault("reason", "CLOSED")));
                yield Decision.event(EventType.DISCONNECT, payload, new ParticipantRow(cmd.sessionId(), actor.userId(),
                        actor.state(), Presence.DISCONNECTED, actor.joinedAt(), actor.leftAt(), ts, null, null));
            }
            case MESSAGE -> {
                requireJoined(actor);
                yield Decision.event(EventType.MESSAGE, Map.of("content", content(in)), null);
            }
            case MESSAGE_EDITED -> {
                requireJoined(actor);
                long target = requireOwnLiveMessage(cmd, session, in);
                yield Decision.event(EventType.MESSAGE_EDITED,
                        orderedMap("targetSeq", target, "content", content(in)), null);
            }
            case MESSAGE_DELETED -> {
                requireJoined(actor);
                long target = requireOwnLiveMessage(cmd, session, in);
                yield Decision.event(EventType.MESSAGE_DELETED, Map.of("targetSeq", target), null);
            }
            case READ -> {
                requireJoined(actor);
                long requested = longValue(in, "upToSeq");
                if (requested < 0) {
                    throw new ApiException(ErrorCode.INVALID_REQUEST, "upToSeq 는 0 이상이어야 합니다.");
                }
                long upTo = Math.min(requested, session.lastSeq());
                yield Decision.event(EventType.READ, Map.of("upToSeq", upTo), null);
            }
            case SESSION_ENDED -> {
                if (!cmd.actorId().equals(session.createdBy()) && actor == null) {
                    throw new ApiException(ErrorCode.NOT_A_PARTICIPANT, "세션 생성자 또는 참여자만 종료할 수 있습니다.");
                }
                Object reason = in.get("reason");
                yield Decision.event(EventType.SESSION_ENDED,
                        reason == null ? Map.of() : Map.of("reason", reason.toString()), null);
            }
            case SESSION_CREATED -> throw new ApiException(ErrorCode.EVENT_TYPE_NOT_ALLOWED,
                    "SESSION_CREATED 는 POST /sessions 로만 생성됩니다.");
        };
    }

    private Decision decideJoin(AppendCommand cmd, SessionRow session, Map<String, ParticipantRow> roster,
                                ParticipantRow actor, Instant ts) {
        if (actor != null && actor.joined()) {
            // 이미 참여 중: 상태 변화가 없으므로 이벤트를 남기지 않는다(상태 기반 멱등).
            return Decision.skip("already joined", null);
        }
        if (!session.invitees().isEmpty() && !session.invitees().contains(cmd.actorId())) {
            throw new ApiException(ErrorCode.NOT_INVITED, "초대되지 않은 사용자입니다.");
        }
        long joinedOthers = roster.values().stream()
                .filter(p -> p.joined() && !p.userId().equals(cmd.actorId()))
                .count();
        if (joinedOthers >= MAX_PARTICIPANTS) {
            throw new ApiException(ErrorCode.SESSION_FULL, "1:1 세션은 최대 " + MAX_PARTICIPANTS + "명까지 참여할 수 있습니다.");
        }
        boolean ws = cmd.viaWebSocket();
        ParticipantRow row = new ParticipantRow(cmd.sessionId(), cmd.actorId(), ParticipantState.JOINED,
                ws ? Presence.ONLINE : Presence.OFFLINE, ts, null, ts,
                ws ? cmd.connectionId() : null, ws ? cmd.instanceId() : null);
        return Decision.event(EventType.JOIN, Map.of("via", ws ? "ws" : "rest"), row);
    }

    /**
     * WebSocket 연결 수립. 참여 전이면 JOIN, 오프라인/단절 상태면 RECONNECT 를 기록한다.
     * 이미 ONLINE 이면(다른 탭/인스턴스에서 연결 인계) 이벤트 없이 연결 정보만 교체한다.
     */
    private Decision decideConnect(AppendCommand cmd, SessionRow session, Map<String, ParticipantRow> roster,
                                   ParticipantRow actor, Instant ts) {
        if (actor == null || !actor.joined()) {
            return decideJoin(cmd, session, roster, actor, ts);
        }
        ParticipantRow row = new ParticipantRow(cmd.sessionId(), actor.userId(), ParticipantState.JOINED,
                Presence.ONLINE, actor.joinedAt(), null, ts, cmd.connectionId(), cmd.instanceId());
        if (actor.presence() == Presence.ONLINE) {
            return Decision.skip("connection takeover", row);
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("connectionId", cmd.connectionId());
        Object resume = cmd.payload() == null ? null : cmd.payload().get("resumeFromSeq");
        if (resume != null) {
            payload.put("resumeFromSeq", resume);
        }
        return Decision.event(EventType.RECONNECT, payload, row);
    }

    private long requireOwnLiveMessage(AppendCommand cmd, SessionRow session, Map<String, Object> in) {
        long target = longValue(in, "targetSeq");
        // 대상 메시지가 아직 없다 = 원본보다 수정/삭제가 먼저 도착한 순서 뒤바뀜.
        // 재시도 가능한 오류로 거절하고, 원본이 저장된 뒤 클라이언트가 재전송하면 정상 반영된다.
        if (target > session.lastSeq()) {
            throw new ApiException(ErrorCode.TARGET_NOT_FOUND, "대상 메시지(seq=" + target + ")가 아직 없습니다.");
        }
        // 이미 지나간 seq 인데 메시지가 아니면 재시도해도 성공할 수 없으므로 재시도 불가 오류로 구분한다.
        SessionEvent original = events.findBySeq(cmd.sessionId(), target).orElse(null);
        if (original == null || original.type() != EventType.MESSAGE) {
            throw new ApiException(ErrorCode.TARGET_NOT_A_MESSAGE, "seq=" + target + " 는 메시지가 아닙니다.");
        }
        if (!original.actorId().equals(cmd.actorId())) {
            throw new ApiException(ErrorCode.TARGET_NOT_OWNED, "본인이 보낸 메시지만 수정/삭제할 수 있습니다.");
        }
        if (events.isMessageDeleted(cmd.sessionId(), target)) {
            throw new ApiException(ErrorCode.TARGET_DELETED, "이미 삭제된 메시지입니다.");
        }
        return target;
    }

    private static void requireJoined(ParticipantRow actor) {
        if (actor == null || !actor.joined()) {
            throw new ApiException(ErrorCode.NOT_A_PARTICIPANT, "세션에 참여 중인 사용자만 보낼 수 있습니다.");
        }
    }

    private static String content(Map<String, Object> in) {
        Object v = in.get("content");
        if (!(v instanceof String s) || s.isBlank()) {
            throw new ApiException(ErrorCode.INVALID_REQUEST, "payload.content 가 필요합니다.");
        }
        if (s.length() > MAX_CONTENT_LENGTH) {
            throw new ApiException(ErrorCode.INVALID_REQUEST, "content 는 " + MAX_CONTENT_LENGTH + "자 이하여야 합니다.");
        }
        return s;
    }

    private static long longValue(Map<String, Object> in, String key) {
        Object v = in.get(key);
        try {
            if (v instanceof Number n) {
                return n.longValue();
            }
            if (v instanceof String s) {
                return Long.parseLong(s);
            }
        } catch (NumberFormatException ignored) {
            // 아래에서 오류 응답
        }
        throw new ApiException(ErrorCode.INVALID_REQUEST, "payload." + key + " (숫자)가 필요합니다.");
    }

    private static Map<String, Object> orderedMap(String k1, Object v1, String k2, Object v2) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(k1, v1);
        m.put(k2, v2);
        return m;
    }

    /** 세션 생성 시 SESSION_CREATED(seq=1)를 기록한다. SessionService 가 같은 트랜잭션에서 호출한다. */
    public SessionEvent recordCreation(SessionRow session) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("createdBy", session.createdBy());
        payload.put("title", session.title());
        payload.put("invitees", List.copyOf(session.invitees()));
        SessionEvent created = new SessionEvent(session.id(), 1, UUID.randomUUID(), "create:" + session.id(),
                EventType.SESSION_CREATED, session.createdBy(), payload, null, session.createdAt());
        events.insert(created);
        outbox.enqueue(session.id(), 1, session.createdAt());
        return created;
    }
}
