package com.chatreplay.session;

import static com.chatreplay.common.Jdbc.instant;
import static com.chatreplay.common.Jdbc.ts;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class ParticipantRepository {

    private final JdbcClient jdbc;

    public ParticipantRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public List<ParticipantRow> findBySession(UUID sessionId) {
        return jdbc.sql("SELECT * FROM session_participants WHERE session_id = :sid ORDER BY joined_at, user_id")
                .param("sid", sessionId)
                .query(ParticipantRepository::map)
                .list();
    }

    public List<ParticipantRow> findBySessions(java.util.Collection<UUID> sessionIds) {
        if (sessionIds.isEmpty()) {
            return List.of();
        }
        return jdbc.sql("SELECT * FROM session_participants WHERE session_id IN (:ids) ORDER BY joined_at, user_id")
                .param("ids", sessionIds)
                .query(ParticipantRepository::map)
                .list();
    }

    public void upsert(ParticipantRow p) {
        jdbc.sql("""
                        INSERT INTO session_participants
                            (session_id, user_id, state, presence, joined_at, left_at, last_seen_at, connection_id, connected_instance)
                        VALUES (:sid, :uid, :state, :presence, :joinedAt, :leftAt, :lastSeenAt, :connId, :instance)
                        ON CONFLICT (session_id, user_id) DO UPDATE
                           SET state = EXCLUDED.state, presence = EXCLUDED.presence, joined_at = EXCLUDED.joined_at,
                               left_at = EXCLUDED.left_at, last_seen_at = EXCLUDED.last_seen_at,
                               connection_id = EXCLUDED.connection_id, connected_instance = EXCLUDED.connected_instance
                        """)
                .param("sid", p.sessionId())
                .param("uid", p.userId())
                .param("state", p.state().name())
                .param("presence", p.presence().name())
                .param("joinedAt", ts(p.joinedAt()))
                .param("leftAt", ts(p.leftAt()))
                .param("lastSeenAt", ts(p.lastSeenAt()))
                .param("connId", p.connectionId())
                .param("instance", p.connectedInstance())
                .update();
    }

    /** 장애 인스턴스(하트비트 만료 또는 미등록)에 붙어 있던 연결 목록. idx_participants_instance 부분 인덱스를 탄다. */
    public List<ParticipantRow> findConnectionsOnDeadInstances(Instant heartbeatDeadline, int limit) {
        return jdbc.sql("""
                        SELECT p.* FROM session_participants p
                          LEFT JOIN server_instances i ON i.instance_id = p.connected_instance
                         WHERE p.connection_id IS NOT NULL
                           AND (i.instance_id IS NULL OR i.heartbeat_at < :deadline)
                         LIMIT :limit
                        """)
                .param("deadline", ts(heartbeatDeadline))
                .param("limit", limit)
                .query(ParticipantRepository::map)
                .list();
    }

    static ParticipantRow map(ResultSet rs, int rowNum) throws SQLException {
        return new ParticipantRow(
                rs.getObject("session_id", UUID.class),
                rs.getString("user_id"),
                ParticipantState.valueOf(rs.getString("state")),
                Presence.valueOf(rs.getString("presence")),
                instant(rs.getTimestamp("joined_at")),
                instant(rs.getTimestamp("left_at")),
                instant(rs.getTimestamp("last_seen_at")),
                rs.getString("connection_id"),
                rs.getString("connected_instance"));
    }
}
