package com.chatreplay.session;

import static com.chatreplay.common.Jdbc.instant;
import static com.chatreplay.common.Jdbc.ts;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class SessionRepository {

    private final JdbcClient jdbc;

    public SessionRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(SessionRow s) {
        jdbc.sql("""
                        INSERT INTO sessions (id, title, status, created_by, invitees, created_at, last_seq, last_event_at)
                        VALUES (:id, :title, :status, :createdBy, CAST(:invitees AS varchar[]), :createdAt, :lastSeq, :lastEventAt)
                        """)
                .param("id", s.id())
                .param("title", s.title())
                .param("status", s.status().name())
                .param("createdBy", s.createdBy())
                .param("invitees", toPgArray(s.invitees()))
                .param("createdAt", ts(s.createdAt()))
                .param("lastSeq", s.lastSeq())
                .param("lastEventAt", ts(s.lastEventAt()))
                .update();
    }

    public Optional<SessionRow> findById(UUID id) {
        return jdbc.sql("SELECT * FROM sessions WHERE id = :id")
                .param("id", id)
                .query(SessionRepository::map)
                .optional();
    }

    /**
     * 세션 행 잠금. 같은 세션에 대한 쓰기를 직렬화해서 seq 를 빈틈없이 단조 증가시킨다.
     * 1:1 세션이라 세션 단위 쓰기 빈도가 낮아 경합이 작고, 서로 다른 세션끼리는 막지 않는다.
     * 또한 "seq N 이 커밋됐다면 N 미만은 모두 커밋됐다" 가 보장되어, 실시간 전송의 빈틈을 DB 로 메울 수 있다.
     */
    public Optional<SessionRow> lockById(UUID id) {
        return jdbc.sql("SELECT * FROM sessions WHERE id = :id FOR UPDATE")
                .param("id", id)
                .query(SessionRepository::map)
                .optional();
    }

    /** 세션별 현재 head(last_seq). PK IN 조회 1번. */
    public Map<UUID, Long> findHeads(java.util.Collection<UUID> ids) {
        if (ids.isEmpty()) {
            return Map.of();
        }
        Map<UUID, Long> heads = new HashMap<>();
        jdbc.sql("SELECT id, last_seq FROM sessions WHERE id IN (:ids)")
                .param("ids", ids)
                .query(rs -> {
                    heads.put(rs.getObject("id", UUID.class), rs.getLong("last_seq"));
                });
        return heads;
    }

    public void updateAfterAppend(UUID id, long lastSeq, Instant lastEventAt, SessionStatus status, Instant endedAt) {
        jdbc.sql("""
                        UPDATE sessions
                           SET last_seq = :lastSeq, last_event_at = :lastEventAt, status = :status,
                               ended_at = COALESCE(ended_at, :endedAt)
                         WHERE id = :id
                        """)
                .param("id", id)
                .param("lastSeq", lastSeq)
                .param("lastEventAt", ts(lastEventAt))
                .param("status", status.name())
                .param("endedAt", ts(endedAt))
                .update();
    }

    /**
     * 세션 목록 조회 (키셋 페이지네이션).
     * OFFSET 대신 (created_at, id) 커서를 써서 깊은 페이지에서도 비용이 일정하다.
     * 인덱스: status 필터가 있으면 idx_sessions_status_created, 없으면 idx_sessions_created.
     * 참여자 필터는 idx_participants_user 로 세미 조인한다.
     */
    public List<SessionRow> search(SessionSearch q) {
        StringBuilder sql = new StringBuilder("SELECT s.* FROM sessions s WHERE 1=1");
        Map<String, Object> params = new HashMap<>();
        if (q.status() != null) {
            sql.append(" AND s.status = :status");
            params.put("status", q.status().name());
        }
        if (q.from() != null) {
            sql.append(" AND s.created_at >= :from");
            params.put("from", ts(q.from()));
        }
        if (q.to() != null) {
            sql.append(" AND s.created_at < :to");
            params.put("to", ts(q.to()));
        }
        if (q.participant() != null) {
            sql.append(" AND EXISTS (SELECT 1 FROM session_participants p"
                    + " WHERE p.user_id = :participant AND p.session_id = s.id)");
            params.put("participant", q.participant());
        }
        if (q.cursorCreatedAt() != null) {
            sql.append(" AND (s.created_at, s.id) < (:cursorCreatedAt, :cursorId)");
            params.put("cursorCreatedAt", ts(q.cursorCreatedAt()));
            params.put("cursorId", q.cursorId());
        }
        sql.append(" ORDER BY s.created_at DESC, s.id DESC LIMIT :limit");
        params.put("limit", q.limit());
        return jdbc.sql(sql.toString()).params(params).query(SessionRepository::map).list();
    }

    static SessionRow map(ResultSet rs, int rowNum) throws SQLException {
        return new SessionRow(
                rs.getObject("id", UUID.class),
                rs.getString("title"),
                SessionStatus.valueOf(rs.getString("status")),
                rs.getString("created_by"),
                fromPgArray(rs.getArray("invitees")),
                instant(rs.getTimestamp("created_at")),
                instant(rs.getTimestamp("ended_at")),
                rs.getLong("last_seq"),
                instant(rs.getTimestamp("last_event_at")));
    }

    private static String toPgArray(List<String> values) {
        if (values == null || values.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            String escaped = values.get(i).replace("\\", "\\\\").replace("\"", "\\\"");
            sb.append('"').append(escaped).append('"');
        }
        return sb.append('}').toString();
    }

    private static List<String> fromPgArray(Array array) throws SQLException {
        if (array == null) {
            return List.of();
        }
        return List.copyOf(Arrays.asList((String[]) array.getArray()));
    }
}
