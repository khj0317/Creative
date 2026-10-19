package com.chatreplay.projection;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

/**
 * 장애 주입용 스위치. 지정한 세션의 프로젝션 처리를 강제로 실패시켜 재시도/백오프/DLQ 흐름을 재현한다.
 * 테스트와 로컬 데모(/admin/faults, chat.fault-injection.enabled=true)에서만 사용한다.
 */
@Component
public class ProjectionFaultInjector {

    private final Set<UUID> failing = ConcurrentHashMap.newKeySet();

    public void failSession(UUID sessionId) {
        failing.add(sessionId);
    }

    public void heal(UUID sessionId) {
        failing.remove(sessionId);
    }

    public void healAll() {
        failing.clear();
    }

    void check(UUID sessionId) {
        if (failing.contains(sessionId)) {
            throw new IllegalStateException("injected projection failure for session " + sessionId);
        }
    }
}
