package com.chatreplay.projection;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.chatreplay.config.ChatProperties;
import com.chatreplay.event.EventRepository;
import com.chatreplay.event.EventType;
import com.chatreplay.event.SessionEvent;
import com.chatreplay.projection.ProjectionRepository.Projection;
import com.chatreplay.replay.RestoreService;
import com.chatreplay.replay.SnapshotRepository;
import com.chatreplay.session.SessionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 세션 요약 프로젝션 갱신 + 스냅샷 자동 생성(비동기 워커가 호출).
 *
 * <p>멱등성: projected_seq(체크포인트)보다 큰 이벤트만 반영하고, 반영과 체크포인트 갱신을 같은 트랜잭션에서 커밋한다.
 * 같은 작업이 두 번 실행되어도(lease 만료 후 재점유, 재시도) 두 번째 실행은 반영할 이벤트가 없어 no-op 이다.
 */
@Service
public class ProjectionService {

    private static final int PREVIEW_LENGTH = 100;

    private final ProjectionRepository projections;
    private final SessionRepository sessions;
    private final EventRepository events;
    private final RestoreService restoreService;
    private final SnapshotRepository snapshots;
    private final ProjectionFaultInjector faults;
    private final ChatProperties properties;
    private final Clock clock;

    public ProjectionService(ProjectionRepository projections, SessionRepository sessions, EventRepository events,
                             RestoreService restoreService, SnapshotRepository snapshots,
                             ProjectionFaultInjector faults, ChatProperties properties, Clock clock) {
        this.projections = projections;
        this.sessions = sessions;
        this.events = events;
        this.restoreService = restoreService;
        this.snapshots = snapshots;
        this.faults = faults;
        this.properties = properties;
        this.clock = clock;
    }

    /** @return 이번 실행 후의 체크포인트(projected_seq) */
    @Transactional
    public long project(UUID sessionId) {
        Instant now = clock.instant();
        projections.insertEmpty(sessionId, now);
        Projection p = projections.lock(sessionId).orElseThrow();
        long head = sessions.findById(sessionId).orElseThrow().lastSeq();
        if (p.projectedSeq() >= head) {
            return p.projectedSeq();
        }
        faults.check(sessionId);

        List<SessionEvent> batch = events.findRange(sessionId, p.projectedSeq(), head,
                properties.projection().eventsPerRun());
        boolean ended = false;
        for (SessionEvent e : batch) {
            p = applySummary(p, e);
            ended |= e.type() == EventType.SESSION_ENDED;
        }
        long projected = batch.isEmpty() ? p.projectedSeq() : batch.getLast().seq();

        // 스냅샷 자동 생성: seq 가 interval 의 배수인 지점마다 + 세션 종료 시점.
        // 위치가 워커 실행 타이밍과 무관하게 결정되므로, 과거 어느 시점을 복원하든 리플레이는 최대 interval-1 개다.
        // 각 스냅샷은 직전 스냅샷(같은 트랜잭션에서 방금 만든 것 포함) + interval 개 리플레이로 계산된다.
        int interval = properties.snapshot().interval();
        long lastSnapshot = p.lastSnapshotSeq();
        for (long at = (lastSnapshot / interval + 1) * interval; at <= projected; at += interval) {
            snapshots.insert(restoreService.restore(sessionId, at, true).state().toData(), now);
            lastSnapshot = at;
        }
        if (ended && projected > lastSnapshot) {
            snapshots.insert(restoreService.restore(sessionId, projected, true).state().toData(), now);
            lastSnapshot = projected;
        }
        projections.update(new Projection(sessionId, projected, p.messageCount(), p.lastMessageSeq(),
                p.lastMessageAt(), p.lastMessagePreview(), lastSnapshot, now));
        return projected;
    }

    private static Projection applySummary(Projection p, SessionEvent e) {
        int count = p.messageCount();
        Long lastSeq = p.lastMessageSeq();
        Instant lastAt = p.lastMessageAt();
        String preview = p.lastMessagePreview();
        switch (e.type()) {
            case MESSAGE -> {
                count++;
                lastSeq = e.seq();
                lastAt = e.serverTs();
                preview = preview(e.stringPayload("content"));
            }
            case MESSAGE_EDITED -> {
                if (lastSeq != null && lastSeq == e.longPayload("targetSeq")) {
                    preview = preview(e.stringPayload("content"));
                }
            }
            case MESSAGE_DELETED -> {
                count = Math.max(0, count - 1);
                if (lastSeq != null && lastSeq == e.longPayload("targetSeq")) {
                    preview = "(삭제된 메시지)";
                }
            }
            default -> {
                // 요약에 영향 없음
            }
        }
        return new Projection(p.sessionId(), e.seq(), count, lastSeq, lastAt, preview, p.lastSnapshotSeq(),
                p.updatedAt());
    }

    private static String preview(String content) {
        if (content == null) {
            return null;
        }
        return content.length() <= PREVIEW_LENGTH ? content : content.substring(0, PREVIEW_LENGTH) + "…";
    }
}
