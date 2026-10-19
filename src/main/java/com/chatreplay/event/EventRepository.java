package com.chatreplay.event;

import static com.chatreplay.common.Jdbc.instant;
import static com.chatreplay.common.Jdbc.ts;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.chatreplay.session.SessionStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

@Repository
public class EventRepository {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final JdbcClient jdbc;
    private final JsonMapper json;

    public EventRepository(JdbcClient jdbc, JsonMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    public void insert(SessionEvent e) {
        jdbc.sql("""
                        INSERT INTO session_events
                            (session_id, seq, event_id, client_event_id, type, actor_id, payload, client_ts, server_ts)
                        VALUES (:sid, :seq, :eventId, :clientEventId, :type, :actorId, CAST(:payload AS jsonb), :clientTs, :serverTs)
                        """)
                .param("sid", e.sessionId())
                .param("seq", e.seq())
                .param("eventId", e.eventId())
                .param("clientEventId", e.clientEventId())
                .param("type", e.type().name())
                .param("actorId", e.actorId())
                .param("payload", json.writeValueAsString(e.payload()))
                .param("clientTs", ts(e.clientTs()))
                .param("serverTs", ts(e.serverTs()))
                .update();
    }

    /**
     * 핫패스 쓰기: 이벤트 저장 + 세션 head/상태 전진 + 프로젝션 outbox 적재를 writable CTE 한 문장으로 실행한다.
     * 부하 테스트에서 병목이 DB CPU 가 아니라 커넥션 점유 시간(왕복 지연)이었기 때문에, 왕복 수를 줄이는 것이 효과적이다.
     * 세션 행은 이미 같은 트랜잭션에서 FOR UPDATE 로 잠겨 있다.
     */
    public void appendAndAdvance(SessionEvent e, SessionStatus status, Instant endedAt,
                                 Instant now) {
        jdbc.sql("""
                        WITH ev AS (
                            INSERT INTO session_events
                                (session_id, seq, event_id, client_event_id, type, actor_id, payload, client_ts, server_ts)
                            VALUES (:sid, :seq, :eventId, :clientEventId, :type, :actorId, CAST(:payload AS jsonb), :clientTs, :serverTs)
                        ), s AS (
                            UPDATE sessions
                               SET last_seq = :seq, last_event_at = :serverTs, status = :status,
                                   ended_at = COALESCE(ended_at, :endedAt)
                             WHERE id = :sid
                        ), o AS (
                            INSERT INTO projection_outbox (session_id, target_seq, attempts, next_attempt_at, enqueued_at)
                            VALUES (:sid, :seq, 0, :now, :now)
                            ON CONFLICT (session_id) DO UPDATE
                               SET target_seq = GREATEST(projection_outbox.target_seq, EXCLUDED.target_seq)
                        )
                        SELECT 1
                        """)
                .param("sid", e.sessionId())
                .param("seq", e.seq())
                .param("eventId", e.eventId())
                .param("clientEventId", e.clientEventId())
                .param("type", e.type().name())
                .param("actorId", e.actorId())
                .param("payload", json.writeValueAsString(e.payload()))
                .param("clientTs", ts(e.clientTs()))
                .param("serverTs", ts(e.serverTs()))
                .param("status", status.name())
                .param("endedAt", ts(endedAt))
                .param("now", ts(now))
                .query(Integer.class)
                .single();
    }

    /** 멱등 키 조회. uq_events_idempotency 유니크 인덱스를 탄다. */
    public Optional<SessionEvent> findByIdempotencyKey(UUID sessionId, String actorId, String clientEventId) {
        return jdbc.sql("""
                        SELECT * FROM session_events
                         WHERE session_id = :sid AND actor_id = :actor AND client_event_id = :key
                        """)
                .param("sid", sessionId)
                .param("actor", actorId)
                .param("key", clientEventId)
                .query(this::map)
                .optional();
    }

    public Optional<SessionEvent> findBySeq(UUID sessionId, long seq) {
        return jdbc.sql("SELECT * FROM session_events WHERE session_id = :sid AND seq = :seq")
                .param("sid", sessionId)
                .param("seq", seq)
                .query(this::map)
                .optional();
    }

    /** 특정 메시지에 삭제 이벤트가 있는지. idx_events_message_ops(부분 표현식 인덱스)를 탄다. */
    public boolean isMessageDeleted(UUID sessionId, long targetSeq) {
        return jdbc.sql("""
                        SELECT EXISTS (
                            SELECT 1 FROM session_events
                             WHERE session_id = :sid AND type = 'MESSAGE_DELETED'
                               AND (payload ->> 'targetSeq')::bigint = :target)
                        """)
                .param("sid", sessionId)
                .param("target", targetSeq)
                .query(Boolean.class)
                .single();
    }

    /**
     * (afterSeq, toSeq] 구간을 seq 순서로 조회. PK(session_id, seq) 범위 스캔이라 세션 크기와 무관하게
     * 읽는 범위만큼만 비용이 든다. 리플레이/재연결 resume/디버깅 조회가 모두 이 쿼리를 쓴다.
     */
    public List<SessionEvent> findRange(UUID sessionId, long afterSeq, long toSeq, int limit) {
        return jdbc.sql("""
                        SELECT * FROM session_events
                         WHERE session_id = :sid AND seq > :after AND seq <= :to
                         ORDER BY seq
                         LIMIT :limit
                        """)
                .param("sid", sessionId)
                .param("after", afterSeq)
                .param("to", toSeq)
                .param("limit", limit)
                .query(this::map)
                .list();
    }

    /** server_ts 구간 조회(디버깅용). idx_events_session_ts 를 탄다. */
    public List<SessionEvent> findByTime(UUID sessionId, Instant from, Instant to, long afterSeq, int limit) {
        return jdbc.sql("""
                        SELECT * FROM session_events
                         WHERE session_id = :sid AND server_ts >= :from AND server_ts <= :to AND seq > :after
                         ORDER BY seq
                         LIMIT :limit
                        """)
                .param("sid", sessionId)
                .param("from", ts(from))
                .param("to", ts(to))
                .param("after", afterSeq)
                .param("limit", limit)
                .query(this::map)
                .list();
    }

    /**
     * 시각 at 을 "그 시각까지 반영된 마지막 seq" 로 변환한다.
     * server_ts 가 seq 와 함께 비감소하므로 (server_ts DESC, seq DESC) 첫 행이 답이며,
     * idx_events_session_ts(session_id, server_ts, seq) 로 Index Only Scan 1행이면 끝난다.
     */
    public Optional<Long> resolveSeqAt(UUID sessionId, Instant at) {
        return jdbc.sql("""
                        SELECT seq FROM session_events
                         WHERE session_id = :sid AND server_ts <= :at
                         ORDER BY server_ts DESC, seq DESC
                         LIMIT 1
                        """)
                .param("sid", sessionId)
                .param("at", ts(at))
                .query(Long.class)
                .optional();
    }

    private SessionEvent map(ResultSet rs, int rowNum) throws SQLException {
        return new SessionEvent(
                rs.getObject("session_id", UUID.class),
                rs.getLong("seq"),
                rs.getObject("event_id", UUID.class),
                rs.getString("client_event_id"),
                EventType.valueOf(rs.getString("type")),
                rs.getString("actor_id"),
                json.readValue(rs.getString("payload"), MAP_TYPE),
                instant(rs.getTimestamp("client_ts")),
                instant(rs.getTimestamp("server_ts")));
    }
}
