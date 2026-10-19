package com.chatreplay.event;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * 저장된 이벤트 1건. 세션 상태의 유일한 진실 원천이다.
 *
 * @param seq       세션 내 순서. 서버가 부여하며 정렬/복원의 유일한 기준이다.
 * @param clientTs  클라이언트가 주장한 시각. 표시/분석용으로만 저장하고 정렬에는 쓰지 않는다.
 * @param serverTs  seq 와 함께 단조 증가(비감소)하는 서버 시각. timeline?at= 을 seq 로 바꿀 때 쓴다.
 */
public record SessionEvent(
        UUID sessionId,
        long seq,
        UUID eventId,
        String clientEventId,
        EventType type,
        String actorId,
        Map<String, Object> payload,
        Instant clientTs,
        Instant serverTs) {

    public long longPayload(String key) {
        Object v = payload.get(key);
        if (v instanceof Number n) {
            return n.longValue();
        }
        if (v instanceof String s) {
            return Long.parseLong(s);
        }
        throw new IllegalStateException("payload." + key + " is missing in event seq=" + seq);
    }

    public String stringPayload(String key) {
        Object v = payload.get(key);
        return v == null ? null : v.toString();
    }
}
