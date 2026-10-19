package com.chatreplay.event;

public enum EventType {
    SESSION_CREATED(false),
    JOIN(true),
    LEAVE(true),
    /** 서버만 기록: WebSocket 연결 종료, 장애 인스턴스 정리 */
    DISCONNECT(false),
    /** 서버만 기록: 참여자가 오프라인 상태에서 실시간 연결을 (다시) 수립 */
    RECONNECT(false),
    MESSAGE(true),
    MESSAGE_EDITED(true),
    MESSAGE_DELETED(true),
    /** 읽음 위치(upToSeq) 갱신. unread count 복원에 쓴다. */
    READ(true),
    SESSION_ENDED(false);

    private final boolean clientSubmittable;

    EventType(boolean clientSubmittable) {
        this.clientSubmittable = clientSubmittable;
    }

    /** POST /sessions/{id}/events 및 WebSocket send 로 클라이언트가 직접 보낼 수 있는 타입인지 */
    public boolean clientSubmittable() {
        return clientSubmittable;
    }
}
