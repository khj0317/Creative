package com.chatreplay.replay;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.chatreplay.common.ApiException;
import com.chatreplay.common.ErrorCode;
import com.chatreplay.config.ChatProperties;
import com.chatreplay.event.EventRepository;
import com.chatreplay.event.SessionEvent;
import com.chatreplay.session.SessionRepository;
import com.chatreplay.session.SessionRow;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 스냅샷 + 이벤트 리플레이 기반 특정 시점 복원.
 * <pre>
 *   1. 목표 seq 결정 (at → resolveSeqAt, 또는 atSeq 직접 지정)
 *   2. 목표 이하 최신 스냅샷 1건 로드 (없으면 빈 상태)
 *   3. (snapshot.seq, target] 구간 이벤트를 seq 순서로 페이지 단위 리플레이
 * </pre>
 * 비용 = O(스냅샷 크기 + 스냅샷 이후 이벤트 수). 스냅샷 주기 N 이면 리플레이 이벤트는 최대 N-1 개다.
 */
@Service
public class RestoreService {

    public enum Strategy { SNAPSHOT_REPLAY, FULL_REPLAY }

    public record Restored(SessionState state, long targetSeq, Strategy strategy, Long snapshotSeq,
                           int replayedEvents, long elapsedMicros) {
    }

    private final SessionRepository sessions;
    private final EventRepository events;
    private final SnapshotRepository snapshots;
    private final ChatProperties properties;
    private final MeterRegistry meters;
    private final Clock clock;

    public RestoreService(SessionRepository sessions, EventRepository events, SnapshotRepository snapshots,
                          ChatProperties properties, MeterRegistry meters, Clock clock) {
        this.sessions = sessions;
        this.events = events;
        this.snapshots = snapshots;
        this.properties = properties;
        this.meters = meters;
        this.clock = clock;
    }

    /** 시각 기준 복원. at 이 없으면 현재(head). */
    @Transactional(readOnly = true)
    public Restored restoreAt(UUID sessionId, Instant at, boolean useSnapshot) {
        SessionRow session = requireSession(sessionId);
        long target = at == null ? session.lastSeq()
                : events.resolveSeqAt(sessionId, at).orElseThrow(() -> new ApiException(ErrorCode.NO_STATE_AT,
                        "해당 시각(" + at + ")에는 세션이 존재하지 않았습니다."));
        return restore(sessionId, target, useSnapshot);
    }

    @Transactional(readOnly = true)
    public Restored restoreAtSeq(UUID sessionId, long atSeq, boolean useSnapshot) {
        SessionRow session = requireSession(sessionId);
        if (atSeq < 1) {
            throw new ApiException(ErrorCode.NO_STATE_AT, "atSeq 는 1 이상이어야 합니다.");
        }
        return restore(sessionId, Math.min(atSeq, session.lastSeq()), useSnapshot);
    }

    /** 이미 검증된 세션에 대해 target seq 까지 복원(워커/스냅샷 생성에서도 사용). */
    public Restored restore(UUID sessionId, long targetSeq, boolean useSnapshot) {
        long started = System.nanoTime();
        SessionState state = null;
        Long snapshotSeq = null;
        if (useSnapshot) {
            var snapshot = snapshots.findLatestAtOrBefore(sessionId, targetSeq);
            if (snapshot.isPresent()) {
                state = SessionState.from(snapshot.get().state());
                snapshotSeq = snapshot.get().seq();
            }
        }
        if (state == null) {
            state = new SessionState(sessionId);
        }
        int replayed = 0;
        int pageSize = properties.replay().pageSize();
        while (state.seq() < targetSeq) {
            List<SessionEvent> page = events.findRange(sessionId, state.seq(), targetSeq, pageSize);
            if (page.isEmpty()) {
                throw new IllegalStateException("events missing between seq " + state.seq() + " and " + targetSeq);
            }
            SessionStateReducer.applyAll(state, page);
            replayed += page.size();
        }
        long micros = (System.nanoTime() - started) / 1_000;
        Strategy strategy = snapshotSeq == null ? Strategy.FULL_REPLAY : Strategy.SNAPSHOT_REPLAY;
        Timer.builder("chat.timeline.restore")
                .tag("strategy", strategy.name())
                .register(meters)
                .record(java.time.Duration.ofNanos(micros * 1_000));
        meters.counter("chat.timeline.replayed.events", "strategy", strategy.name()).increment(replayed);
        return new Restored(state, targetSeq, strategy, snapshotSeq, replayed, micros);
    }

    /** 현재 head(또는 지정 seq) 상태로 스냅샷을 만든다. 이미 있으면 그대로 둔다(결정적이므로 동일). */
    @Transactional
    public SnapshotRepository.SnapshotInfo createSnapshot(UUID sessionId, Long atSeq) {
        SessionRow session = requireSession(sessionId);
        long target = atSeq == null ? session.lastSeq() : Math.max(1, Math.min(atSeq, session.lastSeq()));
        Restored restored = restore(sessionId, target, true);
        snapshots.insert(restored.state().toData(), clock.instant());
        return snapshots.list(sessionId).stream()
                .filter(s -> s.seq() == target)
                .findFirst()
                .orElseThrow();
    }

    private SessionRow requireSession(UUID sessionId) {
        return sessions.findById(sessionId)
                .orElseThrow(() -> new ApiException(ErrorCode.SESSION_NOT_FOUND, "세션이 없습니다: " + sessionId));
    }
}
