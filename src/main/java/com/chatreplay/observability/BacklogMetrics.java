package com.chatreplay.observability;

import java.util.concurrent.atomic.AtomicLong;

import com.chatreplay.projection.ProjectionOutboxRepository;
import com.chatreplay.projection.ProjectionRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 비동기 파이프라인 적체 지표. 스크레이프마다 DB 를 치지 않도록 주기적으로 계산해 캐시한다.
 * <ul>
 *   <li>chat_projection_outbox_pending : 처리 대기 세션 수(증가 추세면 워커 처리량 부족/장애)</li>
 *   <li>chat_projection_dlq            : 미해결 DLQ 수(0 이 아니면 알림)</li>
 *   <li>chat_projection_lag_max        : 세션 head 와 프로젝션 체크포인트 차이의 최댓값(이벤트 수)</li>
 * </ul>
 */
@Component
public class BacklogMetrics {

    private static final Logger log = LoggerFactory.getLogger(BacklogMetrics.class);

    private final ProjectionOutboxRepository outbox;
    private final ProjectionRepository projections;
    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong dlq = new AtomicLong();
    private final AtomicLong lag = new AtomicLong();

    public BacklogMetrics(ProjectionOutboxRepository outbox, ProjectionRepository projections, MeterRegistry meters) {
        this.outbox = outbox;
        this.projections = projections;
        Gauge.builder("chat.projection.outbox.pending", pending, AtomicLong::get).register(meters);
        Gauge.builder("chat.projection.dlq", dlq, AtomicLong::get).register(meters);
        Gauge.builder("chat.projection.lag.max", lag, AtomicLong::get).register(meters);
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 2000)
    public void refresh() {
        try {
            pending.set(outbox.countPending());
            dlq.set(outbox.countUnresolvedDlq());
            lag.set(projections.maxLag());
        } catch (Exception e) {
            log.debug("backlog metrics refresh failed: {}", e.getMessage());
        }
    }
}
