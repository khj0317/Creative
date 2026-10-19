package com.chatreplay.session;

/**
 * ONLINE       : 실시간(WebSocket) 연결이 살아 있음
 * OFFLINE      : 연결 없음(REST 로만 참여했거나 정상 퇴장)
 * DISCONNECTED : 참여 중 연결이 끊김(네트워크 단절, 서버 장애 등). 세션은 INTERRUPTED 가 된다.
 */
public enum Presence {
    ONLINE,
    OFFLINE,
    DISCONNECTED
}
