package com.chatreplay.event;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.chatreplay.common.ApiException;
import com.chatreplay.common.ErrorCode;
import com.chatreplay.config.InstanceId;
import com.chatreplay.realtime.BroadcastEnvelope;
import com.chatreplay.realtime.SessionBroadcaster;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

/**
 * 이벤트 추가 진입점(REST / WebSocket / 시스템 공통).
 * 트랜잭션(EventAppender) 커밋 이후에만 브로드캐스트하므로, 클라이언트가 받은 이벤트는 반드시 DB 에 있다.
 */
@Service
public class EventService {

    private static final Logger log = LoggerFactory.getLogger(EventService.class);

    public enum ItemStatus { CREATED, DUPLICATE, SKIPPED, REJECTED, FAILED }

    public record ItemResult(String clientEventId, ItemStatus status, Long seq, String serverTs, String code,
                             String message, boolean retryable) {
    }

    private final EventAppender appender;
    private final EventRepository events;
    private final SessionBroadcaster broadcaster;
    private final InstanceId instanceId;
    private final MeterRegistry meters;

    public EventService(EventAppender appender, EventRepository events, SessionBroadcaster broadcaster,
                        InstanceId instanceId, MeterRegistry meters) {
        this.appender = appender;
        this.events = events;
        this.broadcaster = broadcaster;
        this.instanceId = instanceId;
        this.meters = meters;
    }

    public AppendResult append(AppendCommand cmd) {
        Timer.Sample sample = Timer.start(meters);
        AppendResult result;
        try {
            result = appender.append(cmd);
        } catch (DuplicateKeyException e) {
            // 세션 행 잠금으로 직렬화되므로 정상적으로는 오지 않는다. 유니크 인덱스는 최후 방어선.
            SessionEvent existing = events.findByIdempotencyKey(cmd.sessionId(), cmd.actorId(), cmd.clientEventId())
                    .orElseThrow(() -> e);
            result = AppendResult.duplicate(existing, existing.seq(), null);
        } catch (ApiException e) {
            meters.counter("chat.events.rejected", "type", cmd.type().name(), "code", e.code().name()).increment();
            throw e;
        } finally {
            sample.stop(meters.timer("chat.events.append", "type", cmd.type().name()));
        }
        meters.counter("chat.events.ingested", "type", cmd.type().name(), "outcome", result.outcome().name())
                .increment();
        if (result.appended()) {
            broadcaster.publish(BroadcastEnvelope.event(result.event(), instanceId.value()));
        }
        return result;
    }

    /**
     * 배치 수집. 항목마다 독립 트랜잭션이라 일부 실패해도 앞선 항목은 확정되며, 결과는 항목별로 돌려준다.
     * 실패 항목은 같은 clientEventId 로 그대로 재전송하면 된다(성공한 항목이 섞여 있어도 멱등).
     */
    public List<ItemResult> ingest(UUID sessionId, String actorId, List<EventRequest> requests) {
        List<ItemResult> results = new ArrayList<>(requests.size());
        for (EventRequest r : requests) {
            if (!r.type().clientSubmittable()) {
                results.add(rejected(r.clientEventId(), new ApiException(ErrorCode.EVENT_TYPE_NOT_ALLOWED,
                        r.type() + " 는 클라이언트가 보낼 수 없는 타입입니다.")));
                continue;
            }
            try {
                AppendResult res = append(AppendCommand.rest(sessionId, actorId, r.type(), r.clientEventId(),
                        r.payload(), r.clientTs()));
                results.add(switch (res.outcome()) {
                    case APPENDED -> new ItemResult(r.clientEventId(), ItemStatus.CREATED, res.event().seq(),
                            res.event().serverTs().toString(), null, null, false);
                    case DUPLICATE -> new ItemResult(r.clientEventId(), ItemStatus.DUPLICATE, res.event().seq(),
                            res.event().serverTs().toString(), null, res.reason(), false);
                    case SKIPPED -> new ItemResult(r.clientEventId(), ItemStatus.SKIPPED, null, null, null,
                            res.reason(), false);
                });
            } catch (ApiException e) {
                results.add(rejected(r.clientEventId(), e));
            } catch (DataAccessException e) {
                log.error("event ingest failed for {}: {}", r.clientEventId(), e.getMessage());
                results.add(new ItemResult(r.clientEventId(), ItemStatus.FAILED, null, null,
                        ErrorCode.SERVICE_UNAVAILABLE.name(), "저장소 오류", true));
            }
        }
        return results;
    }

    private static ItemResult rejected(String clientEventId, ApiException e) {
        return new ItemResult(clientEventId, ItemStatus.REJECTED, null, null, e.code().name(), e.getMessage(),
                e.code().retryable());
    }
}
