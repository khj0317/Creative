package com.chatreplay.replay;

import static com.chatreplay.common.Jdbc.instant;
import static com.chatreplay.common.Jdbc.ts;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.json.JsonMapper;

@Repository
public class SnapshotRepository {

    public record Snapshot(UUID sessionId, long seq, int stateVersion, SessionState.Data state, int eventCount,
                           Instant createdAt) {
    }

    public record SnapshotInfo(long seq, int stateVersion, int eventCount, int sizeBytes, Instant createdAt) {
    }

    private final JdbcClient jdbc;
    private final JsonMapper json;

    public SnapshotRepository(JdbcClient jdbc, JsonMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    /**
     * target 이하에서 가장 최근 스냅샷. PK(session_id, seq) 를 역방향으로 1행만 읽는다.
     * 호환되지 않는 버전은 건너뛴다(형식 변경 후에도 잘못된 상태로 복원하지 않기 위함).
     */
    public Optional<Snapshot> findLatestAtOrBefore(UUID sessionId, long targetSeq) {
        return jdbc.sql("""
                        SELECT * FROM session_snapshots
                         WHERE session_id = :sid AND seq <= :target AND state_version = :version
                         ORDER BY seq DESC
                         LIMIT 1
                        """)
                .param("sid", sessionId)
                .param("target", targetSeq)
                .param("version", SessionState.VERSION)
                .query((rs, n) -> new Snapshot(
                        rs.getObject("session_id", UUID.class),
                        rs.getLong("seq"),
                        rs.getInt("state_version"),
                        json.readValue(rs.getString("state"), SessionState.Data.class),
                        rs.getInt("event_count"),
                        instant(rs.getTimestamp("created_at"))))
                .optional();
    }

    /** 같은 (세션, seq) 스냅샷은 내용이 결정적으로 같으므로 중복 생성은 무시한다(워커 중복 실행에도 안전). */
    public boolean insert(SessionState.Data state, Instant now) {
        int rows = jdbc.sql("""
                        INSERT INTO session_snapshots (session_id, seq, state_version, state, event_count, created_at)
                        VALUES (:sid, :seq, :version, CAST(:state AS jsonb), :eventCount, :createdAt)
                        ON CONFLICT (session_id, seq) DO NOTHING
                        """)
                .param("sid", state.sessionId())
                .param("seq", state.seq())
                .param("version", SessionState.VERSION)
                .param("state", json.writeValueAsString(state))
                .param("eventCount", (int) state.appliedEvents())
                .param("createdAt", ts(now))
                .update();
        return rows > 0;
    }

    public List<SnapshotInfo> list(UUID sessionId) {
        return jdbc.sql("""
                        SELECT seq, state_version, event_count, pg_column_size(state) AS size_bytes, created_at
                          FROM session_snapshots WHERE session_id = :sid ORDER BY seq DESC
                        """)
                .param("sid", sessionId)
                .query((rs, n) -> new SnapshotInfo(rs.getLong("seq"), rs.getInt("state_version"),
                        rs.getInt("event_count"), rs.getInt("size_bytes"), instant(rs.getTimestamp("created_at"))))
                .list();
    }
}
