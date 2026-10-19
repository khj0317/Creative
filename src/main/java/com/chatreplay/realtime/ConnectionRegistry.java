package com.chatreplay.realtime;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/** 이 인스턴스에 붙어 있는 WebSocket 연결 목록(세션별). 브로드캐스트 수신 시 로컬 전달 대상이다. */
@Component
public class ConnectionRegistry {

    private final Map<UUID, Set<ClientConnection>> bySession = new ConcurrentHashMap<>();

    public ConnectionRegistry(MeterRegistry meters) {
        Gauge.builder("chat.ws.connections", this, ConnectionRegistry::size).register(meters);
        Gauge.builder("chat.ws.sessions", bySession, Map::size).register(meters);
    }

    /** @return 이 인스턴스에서 해당 세션의 첫 연결이면 true */
    public boolean add(ClientConnection conn) {
        boolean[] first = {false};
        bySession.compute(conn.sessionId(), (id, set) -> {
            if (set == null) {
                set = ConcurrentHashMap.newKeySet();
                first[0] = true;
            }
            set.add(conn);
            return set;
        });
        return first[0];
    }

    /** @return 해당 세션의 마지막 연결이 빠졌으면 true */
    public boolean remove(ClientConnection conn) {
        boolean[] last = {false};
        bySession.computeIfPresent(conn.sessionId(), (id, set) -> {
            set.remove(conn);
            if (set.isEmpty()) {
                last[0] = true;
                return null;
            }
            return set;
        });
        return last[0];
    }

    public Collection<ClientConnection> connections(UUID sessionId) {
        Set<ClientConnection> set = bySession.get(sessionId);
        return set == null ? List.of() : List.copyOf(set);
    }

    public Set<UUID> sessionIds() {
        return Set.copyOf(bySession.keySet());
    }

    public List<ClientConnection> all() {
        return bySession.values().stream().flatMap(Set::stream).toList();
    }

    public int size() {
        return bySession.values().stream().mapToInt(Set::size).sum();
    }
}
