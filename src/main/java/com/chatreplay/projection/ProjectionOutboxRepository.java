package com.chatreplay.projection;

import static com.chatreplay.common.Jdbc.instant;
import static com.chatreplay.common.Jdbc.ts;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Transactional Outbox. 이벤트 저장 트랜잭션에서 세션당 1행으로 "프로젝션 필요" 신호를 남긴다.
 * 워커는 lease(locked_until) 방식으로 행을 점유하므로 처리 중에 DB 행 잠금을 오래 잡지 않는다.
 */
@Repository
public class ProjectionOutboxRepository {

    public record Claimed(UUID sessionId, long targetSeq, int attempts) {
    }

    public record DlqEntry(long id, UUID sessionId, long targetSeq, int attempts, String lastError, Instant failedAt,
                           Instant resolvedAt) {
    }

    private final JdbcClient jdbc;

    public ProjectionOutboxRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** 세션당 1행으로 합친다(coalescing): 메시지가 100개 와도 워커 작업은 1건이다. */
    public void enqueue(UUID sessionId, long targetSeq, Instant now) {
        jdbc.sql("""
                        INSERT INTO projection_outbox (session_id, target_seq, attempts, next_attempt_at, enqueued_at)
                        VALUES (:sid, :seq, 0, :now, :now)
                        ON CONFLICT (session_id) DO UPDATE
                           SET target_seq = GREATEST(projection_outbox.target_seq, EXCLUDED.target_seq)
                        """)
                .param("sid", sessionId)
                .param("seq", targetSeq)
                .param("now", ts(now))
                .update();
    }

    /**
     * 처리할 행을 lease 로 점유한다. FOR UPDATE SKIP LOCKED 로 여러 인스턴스의 워커가 같은 행을 동시에 잡지 않고,
     * lease 가 만료된 행(점유한 워커가 죽은 경우)은 다른 워커가 다시 가져간다.
     */
    public List<Claimed> claim(String owner, Instant now, Instant leaseUntil, int limit) {
        return jdbc.sql("""
                        UPDATE projection_outbox o
                           SET locked_by = :owner, locked_until = :leaseUntil
                         WHERE o.session_id IN (
                               SELECT session_id FROM projection_outbox
                                WHERE next_attempt_at <= :now
                                  AND (locked_until IS NULL OR locked_until < :now)
                                ORDER BY next_attempt_at
                                LIMIT :limit
                                FOR UPDATE SKIP LOCKED)
                        RETURNING o.session_id, o.target_seq, o.attempts
                        """)
                .param("owner", owner)
                .param("leaseUntil", ts(leaseUntil))
                .param("now", ts(now))
                .param("limit", limit)
                .query((rs, n) -> new Claimed(rs.getObject("session_id", UUID.class), rs.getLong("target_seq"),
                        rs.getInt("attempts")))
                .list();
    }

    /** 처리 완료. 처리 중 새 이벤트가 들어와 target_seq 가 올라갔다면 지우지 않고 lease 만 풀어 다음 주기에 이어서 처리한다. */
    public void complete(UUID sessionId, String owner, long processedSeq) {
        int deleted = jdbc.sql("""
                        DELETE FROM projection_outbox
                         WHERE session_id = :sid AND locked_by = :owner AND target_seq <= :processed
                        """)
                .param("sid", sessionId)
                .param("owner", owner)
                .param("processed", processedSeq)
                .update();
        if (deleted == 0) {
            jdbc.sql("""
                            UPDATE projection_outbox SET locked_by = NULL, locked_until = NULL, attempts = 0, last_error = NULL
                             WHERE session_id = :sid AND locked_by = :owner
                            """)
                    .param("sid", sessionId)
                    .param("owner", owner)
                    .update();
        }
    }

    public void scheduleRetry(UUID sessionId, String owner, int attempts, Instant nextAttemptAt, String error) {
        jdbc.sql("""
                        UPDATE projection_outbox
                           SET attempts = :attempts, next_attempt_at = :next, last_error = :error,
                               locked_by = NULL, locked_until = NULL
                         WHERE session_id = :sid AND locked_by = :owner
                        """)
                .param("sid", sessionId)
                .param("owner", owner)
                .param("attempts", attempts)
                .param("next", ts(nextAttemptAt))
                .param("error", truncate(error))
                .update();
    }

    /** 최대 재시도 초과: DLQ 로 옮기고 outbox 에서 제거한다(같은 트랜잭션에서 호출). */
    public void moveToDlq(UUID sessionId, String owner, long targetSeq, int attempts, String error, Instant now) {
        jdbc.sql("""
                        INSERT INTO projection_dlq (session_id, target_seq, attempts, last_error, failed_at)
                        VALUES (:sid, :seq, :attempts, :error, :now)
                        """)
                .param("sid", sessionId)
                .param("seq", targetSeq)
                .param("attempts", attempts)
                .param("error", truncate(error))
                .param("now", ts(now))
                .update();
        jdbc.sql("DELETE FROM projection_outbox WHERE session_id = :sid AND locked_by = :owner")
                .param("sid", sessionId)
                .param("owner", owner)
                .update();
    }

    public List<DlqEntry> findUnresolvedDlq(int limit) {
        return jdbc.sql("""
                        SELECT * FROM projection_dlq WHERE resolved_at IS NULL ORDER BY failed_at LIMIT :limit
                        """)
                .param("limit", limit)
                .query((rs, n) -> new DlqEntry(rs.getLong("id"), rs.getObject("session_id", UUID.class),
                        rs.getLong("target_seq"), rs.getInt("attempts"), rs.getString("last_error"),
                        instant(rs.getTimestamp("failed_at")), instant(rs.getTimestamp("resolved_at"))))
                .list();
    }

    /** DLQ 항목을 해결 처리하고 outbox 에 다시 넣는다(같은 트랜잭션에서 호출). */
    public boolean redrive(long dlqId, Instant now) {
        var entry = jdbc.sql("""
                        UPDATE projection_dlq SET resolved_at = :now
                         WHERE id = :id AND resolved_at IS NULL
                        RETURNING session_id, target_seq
                        """)
                .param("id", dlqId)
                .param("now", ts(now))
                .query((rs, n) -> new Object[]{rs.getObject("session_id", UUID.class), rs.getLong("target_seq")})
                .optional();
        if (entry.isEmpty()) {
            return false;
        }
        enqueue((UUID) entry.get()[0], (Long) entry.get()[1], now);
        return true;
    }

    public long countPending() {
        return jdbc.sql("SELECT count(*) FROM projection_outbox").query(Long.class).single();
    }

    public long countUnresolvedDlq() {
        return jdbc.sql("SELECT count(*) FROM projection_dlq WHERE resolved_at IS NULL").query(Long.class).single();
    }

    private static String truncate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() > 2000 ? s.substring(0, 2000) : s;
    }
}
