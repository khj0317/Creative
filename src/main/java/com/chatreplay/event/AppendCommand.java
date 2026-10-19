package com.chatreplay.event;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * 이벤트 추가 요청.
 *
 * @param clientEventId 멱등 키. 같은 (세션, 사용자, 키) 로 다시 오면 저장하지 않고 기존 결과를 돌려준다.
 * @param connectionId  WebSocket 연결 ID. JOIN/RECONNECT 는 이 연결을 참여자에 붙이고,
 *                      DISCONNECT 는 "현재 붙어 있는 연결이 이 연결일 때만" 기록한다(늦게 도착한 종료 무시).
 * @param via           "rest" | "ws" | "system". JOIN 시 presence 결정과 감사 로그에 쓴다.
 */
public record AppendCommand(
        UUID sessionId,
        String actorId,
        EventType type,
        String clientEventId,
        Map<String, Object> payload,
        Instant clientTs,
        String connectionId,
        String instanceId,
        String via) {

    public static AppendCommand rest(UUID sessionId, String actorId, EventType type, String clientEventId,
                                     Map<String, Object> payload, Instant clientTs) {
        return new AppendCommand(sessionId, actorId, type, clientEventId,
                payload == null ? Map.of() : payload, clientTs, null, null, "rest");
    }

    public boolean viaWebSocket() {
        return "ws".equals(via);
    }
}
