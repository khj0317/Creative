package com.chatreplay.projection;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.chatreplay.config.ChatProperties;
import com.chatreplay.config.InstanceId;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 비동기 프로젝션 워커. 모든 인스턴스에서 돌며 outbox 를 lease 로 나눠 가진다.
 * <pre>
 *   claim(SKIP LOCKED + lease) → project(트랜잭션, 체크포인트 멱등) → complete
 *                                      └ 실패: 지수 백오프(+jitter) 재시도 → max 초과 시 DLQ
 * </pre>
 */
@Component
public class ProjectionWorker {

    private static final Logger log = LoggerFactory.getLogger(ProjectionWorker.class);

    private final ProjectionOutboxRepository outbox;
    private final ProjectionService projectionService;
    private final TransactionTemplate tx;
    private final ChatProperties.Projection config;
    private final String owner;
    private final Clock clock;
    private final MeterRegistry meters;

    public ProjectionWorker(ProjectionOutboxRepository outbox, ProjectionService projectionService,
                            TransactionTemplate tx, ChatProperties properties, InstanceId instanceId, Clock clock,
                            MeterRegistry meters) {
        this.outbox = outbox;
        this.projectionService = projectionService;
        this.tx = tx;
        this.config = properties.projection();
        this.owner = instanceId.value();
        this.clock = clock;
        this.meters = meters;
    }

    @Scheduled(fixedDelayString = "${chat.projection.poll-interval}", initialDelay = 1000)
    public void poll() {
        try {
            runOnce();
        } catch (Exception e) {
            // DB 장애 등: 다음 주기에 다시 시도한다(점유했던 lease 는 만료되면 자동 반환).
            log.warn("projection poll failed: {}", e.getMessage());
        }
    }

    /** 한 번 처리하고 처리한 건수를 돌려준다(테스트에서 직접 호출). */
    public int runOnce() {
        Instant now = clock.instant();
        List<ProjectionOutboxRepository.Claimed> claimed =
                outbox.claim(owner, now, now.plus(config.lease()), config.batchSize());
        for (var item : claimed) {
            process(item);
        }
        return claimed.size();
    }

    private void process(ProjectionOutboxRepository.Claimed item) {
        MDC.put("sessionId", item.sessionId().toString());
        try {
            long projected = projectionService.project(item.sessionId());
            outbox.complete(item.sessionId(), owner, projected);
            meters.counter("chat.projection.processed", "result", "success").increment();
        } catch (Exception e) {
            int attempts = item.attempts() + 1;
            String error = e.getClass().getSimpleName() + ": " + e.getMessage();
            if (attempts >= config.maxAttempts()) {
                log.error("projection moved to DLQ after {} attempts: {}", attempts, error);
                tx.executeWithoutResult(s -> outbox.moveToDlq(item.sessionId(), owner, item.targetSeq(), attempts,
                        error, clock.instant()));
                meters.counter("chat.projection.processed", "result", "dlq").increment();
            } else {
                Duration delay = backoff(attempts);
                log.warn("projection failed (attempt {}/{}), retry in {}ms: {}", attempts, config.maxAttempts(),
                        delay.toMillis(), error);
                outbox.scheduleRetry(item.sessionId(), owner, attempts, clock.instant().plus(delay), error);
                meters.counter("chat.projection.processed", "result", "retry").increment();
            }
        } finally {
            MDC.remove("sessionId");
        }
    }

    /** 지수 백오프 + equal jitter: exp = min(max, base * 2^(n-1)), 지연 = exp/2 + rand(0..exp/2). 동시 재시도 폭주를 막는다. */
    Duration backoff(int attempts) {
        long base = config.backoffBase().toMillis();
        long cap = config.backoffMax().toMillis();
        long exp = Math.min(cap, base * (1L << Math.min(attempts - 1, 20)));
        long jittered = exp / 2 + ThreadLocalRandom.current().nextLong(exp / 2 + 1);
        return Duration.ofMillis(jittered);
    }
}
