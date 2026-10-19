package com.chatreplay.realtime;

import java.util.Map;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;

/**
 * 브로드캐스트 메시지를 이 인스턴스의 로컬 연결들에 전달한다.
 * 이벤트는 연결별 메일박스에 넣기만 하므로 수신 스레드(Redis 리스너 등)는 막히지 않는다.
 * 순서는 ClientConnection 의 seq 규칙이 보장한다.
 */
@Component
public class LocalDispatcher {

    public static final CloseStatus REPLACED = new CloseStatus(4001, "replaced by a newer connection");

    private final ConnectionRegistry registry;

    public LocalDispatcher(ConnectionRegistry registry) {
        this.registry = registry;
    }

    public void dispatch(BroadcastEnvelope env) {
        for (ClientConnection conn : registry.connections(env.sessionId())) {
            switch (env.kind()) {
                case EVENT -> conn.enqueue(env.event());
                case TYPING -> {
                    if (!conn.userId().equals(env.userId())) {
                        conn.send(Map.of("type", "typing",
                                "userId", env.userId(), "isTyping", Boolean.TRUE.equals(env.typing())));
                    }
                }
                case REPLACED -> {
                    if (conn.userId().equals(env.userId()) && !conn.connectionId().equals(env.connectionId())) {
                        conn.markReplaced();
                        conn.close(REPLACED);
                    }
                }
            }
        }
    }
}
