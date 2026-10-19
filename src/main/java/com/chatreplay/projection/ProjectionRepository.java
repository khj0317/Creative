package com.chatreplay.projection;

import static com.chatreplay.common.Jdbc.instant;
import static com.chatreplay.common.Jdbc.ts;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class ProjectionRepository {

    public record Projection(UUID sessionId, long projectedSeq, int messageCount, Long lastMessageSeq,
                             Instant lastMessageAt, String lastMessagePreview, long lastSnapshotSeq,
                             Instant updatedAt) {
    }

    private final JdbcClient jdbc;

    public ProjectionRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insertEmpty(UUID sessionId, Instant now) {
        jdbc.sql("""
                        INSERT INTO session_projections (session_id, updated_at) VALUES (:sid, :now)
                        ON CONFLICT (session_id) DO NOTHING
                        """)
                .param("sid", sessionId)
                .param("now", ts(now))
                .update();
    }

    /** 프로젝션 행 잠금. 같은 세션을 두 워커가 동시에 처리하더라도(lease 만료 등) 여기서 직렬화된다. */
    public Optional<Projection> lock(UUID sessionId) {
        return jdbc.sql("SELECT * FROM session_projections WHERE session_id = :sid FOR UPDATE")
                .param("sid", sessionId)
                .query(ProjectionRepository::map)
                .optional();
    }

    public Optional<Projection> find(UUID sessionId) {
        return jdbc.sql("SELECT * FROM session_projections WHERE session_id = :sid")
                .param("sid", sessionId)
                .query(ProjectionRepository::map)
                .optional();
    }

    public List<Projection> findAll(Collection<UUID> sessionIds) {
        if (sessionIds.isEmpty()) {
            return List.of();
        }
        return jdbc.sql("SELECT * FROM session_projections WHERE session_id IN (:ids)")
                .param("ids", sessionIds)
                .query(ProjectionRepository::map)
                .list();
    }

    public void update(Projection p) {
        jdbc.sql("""
                        UPDATE session_projections
                           SET projected_seq = :seq, message_count = :count, last_message_seq = :lastSeq,
                               last_message_at = :lastAt, last_message_preview = :preview,
                               last_snapshot_seq = :snapSeq, updated_at = :updatedAt
                         WHERE session_id = :sid
                        """)
                .param("sid", p.sessionId())
                .param("seq", p.projectedSeq())
                .param("count", p.messageCount())
                .param("lastSeq", p.lastMessageSeq())
                .param("lastAt", ts(p.lastMessageAt()))
                .param("preview", p.lastMessagePreview())
                .param("snapSeq", p.lastSnapshotSeq())
                .param("updatedAt", ts(p.updatedAt()))
                .update();
    }

    /** 재구축: 체크포인트를 0으로 돌리면 워커가 처음부터 다시 계산한다(결정적이므로 결과 동일). */
    public void reset(UUID sessionId, Instant now) {
        jdbc.sql("""
                        UPDATE session_projections
                           SET projected_seq = 0, message_count = 0, last_message_seq = NULL, last_message_at = NULL,
                               last_message_preview = NULL, updated_at = :now
                         WHERE session_id = :sid
                        """)
                .param("sid", sessionId)
                .param("now", ts(now))
                .update();
    }

    /** 세션별 head 와 프로젝션 체크포인트의 차이(전체 합/최대). 관측용. */
    public long maxLag() {
        return jdbc.sql("""
                        SELECT COALESCE(MAX(s.last_seq - p.projected_seq), 0)
                          FROM projection_outbox o
                          JOIN sessions s ON s.id = o.session_id
                          JOIN session_projections p ON p.session_id = o.session_id
                        """)
                .query(Long.class)
                .single();
    }

    private static Projection map(ResultSet rs, int n) throws SQLException {
        long lastMessageSeq = rs.getLong("last_message_seq");
        boolean noLastMessage = rs.wasNull();
        return new Projection(
                rs.getObject("session_id", UUID.class),
                rs.getLong("projected_seq"),
                rs.getInt("message_count"),
                noLastMessage ? null : lastMessageSeq,
                instant(rs.getTimestamp("last_message_at")),
                rs.getString("last_message_preview"),
                rs.getLong("last_snapshot_seq"),
                instant(rs.getTimestamp("updated_at")));
    }
}
