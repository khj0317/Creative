package com.chatreplay.realtime;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.chatreplay.session.SessionRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 실시간 전달 안전망. 주기적으로 이 인스턴스가 들고 있는 세션들의 head 를 한 번에 조회하고,
 * 뒤처진 연결은 DB 에서 따라잡게 한다. Pub/Sub 유실(구독 확정 전 발행, Redis 재시작 등)의 최대 지연을 주기로 제한한다.
 * 비용: 주기당 쿼리 1번(PK IN), 정상 상황에서는 아무것도 보내지 않는다.
 */
@Component
public class DeliverySafetyNet {

    private static final Logger log = LoggerFactory.getLogger(DeliverySafetyNet.class);

    private final ConnectionRegistry registry;
    private final SessionRepository sessions;
    private final Counter resyncs;

    public DeliverySafetyNet(ConnectionRegistry registry, SessionRepository sessions, MeterRegistry meters) {
        this.registry = registry;
        this.sessions = sessions;
        this.resyncs = meters.counter("chat.delivery.resync");
    }

    @Scheduled(fixedDelay = 2000, initialDelay = 2000)
    public void sync() {
        try {
            Set<UUID> ids = registry.sessionIds();
            if (ids.isEmpty()) {
                return;
            }
            Map<UUID, Long> heads = sessions.findHeads(ids);
            for (ClientConnection conn : registry.all()) {
                Long head = heads.get(conn.sessionId());
                if (head != null && head > conn.lastSentSeq()) {
                    // 직전 주기 이후 커밋된 이벤트가 아직 전달 중일 수도 있으므로 카운터는 '유실 의심' 지표다.
                    resyncs.increment();
                    conn.syncTo(head);
                }
            }
        } catch (Exception e) {
            log.debug("delivery safety net failed: {}", e.getMessage());
        }
    }
}
