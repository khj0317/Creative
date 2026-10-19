package com.chatreplay.realtime;

import java.util.UUID;

import com.chatreplay.event.SessionEvent;

/**
 * 인스턴스 간 전달 단위.
 * EVENT     : 저장(커밋)된 이벤트. 받는 쪽은 seq 기준으로 중복 제거/빈틈 메우기를 한다.
 * TYPING    : 입력 중 표시. 저장하지 않는 휘발성 신호(복원 대상 아님).
 * REPLACED  : 같은 사용자가 새 연결을 맺음 → 다른 인스턴스에 남은 이전 연결을 닫는다.
 */
public record BroadcastEnvelope(Kind kind, UUID sessionId, SessionEvent event, String userId, Boolean typing,
                                String connectionId, String originInstance) {

    public enum Kind { EVENT, TYPING, REPLACED }

    public static BroadcastEnvelope event(SessionEvent e, String origin) {
        return new BroadcastEnvelope(Kind.EVENT, e.sessionId(), e, null, null, null, origin);
    }

    public static BroadcastEnvelope typing(UUID sessionId, String userId, boolean typing, String connectionId,
                                           String origin) {
        return new BroadcastEnvelope(Kind.TYPING, sessionId, null, userId, typing, connectionId, origin);
    }

    public static BroadcastEnvelope replaced(UUID sessionId, String userId, String newConnectionId, String origin) {
        return new BroadcastEnvelope(Kind.REPLACED, sessionId, null, userId, null, newConnectionId, origin);
    }
}
