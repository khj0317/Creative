package com.chatreplay.replay;

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
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Timeline", description = "이벤트 기반 특정 시점 상태 복원 / 이벤트 조회 / 스냅샷")
@RestController
@RequestMapping("/sessions/{id}")
public class TimelineController {

    public enum Mode { AUTO, FULL }

    private final RestoreService restoreService;
    private final SnapshotRepository snapshots;
    private final EventRepository events;
    private final SessionRepository sessions;
    private final ChatProperties properties;

    public TimelineController(RestoreService restoreService, SnapshotRepository snapshots, EventRepository events,
                              SessionRepository sessions, ChatProperties properties) {
        this.restoreService = restoreService;
        this.snapshots = snapshots;
        this.events = events;
        this.sessions = sessions;
        this.properties = properties;
    }

    @Operation(summary = "특정 시점 상태 복원",
            description = """
                    at(서버 시각) 또는 atSeq 시점의 참여자/presence/메시지(수정·삭제 반영)/unread 를 복원한다.
                    둘 다 없으면 현재(head). mode=AUTO 는 스냅샷+리플레이, FULL 은 전체 리플레이(검증용).
                    at 은 "그 시각까지 서버가 커밋한 마지막 seq" 로 변환된다(server_ts 는 seq 와 함께 단조 증가).""")
    @GetMapping("/timeline")
    public TimelineResponse timeline(
            @PathVariable UUID id,
            @Parameter(description = "복원 시각(ISO-8601, 서버 시각 기준)")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant at,
            @Parameter(description = "복원 seq (at 보다 우선)")
            @RequestParam(required = false) Long atSeq,
            @Parameter(description = "응답에 담을 최근 메시지 수(기본 50, 최대 500)")
            @RequestParam(required = false) Integer messageLimit,
            @RequestParam(defaultValue = "AUTO") Mode mode) {
        boolean useSnapshot = mode == Mode.AUTO;
        RestoreService.Restored restored = atSeq != null
                ? restoreService.restoreAtSeq(id, atSeq, useSnapshot)
                : restoreService.restoreAt(id, at, useSnapshot);
        int limit = messageLimit == null ? properties.replay().defaultMessageLimit()
                : Math.min(Math.max(messageLimit, 0), 500);
        return TimelineResponse.of(restored, at, limit);
    }

    public record EventPage(List<SessionEvent> events, Long nextAfterSeq, long headSeq) {
    }

    @Operation(summary = "이벤트 조회(디버깅/검증)",
            description = "afterSeq/toSeq(seq 구간) 또는 from/to(server_ts 구간). seq 오름차순, nextAfterSeq 로 이어서 조회.")
    @GetMapping("/events")
    public EventPage events(
            @PathVariable UUID id,
            @RequestParam(defaultValue = "0") long afterSeq,
            @RequestParam(required = false) Long toSeq,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(defaultValue = "100") int limit) {
        SessionRow session = sessions.findById(id)
                .orElseThrow(() -> new ApiException(ErrorCode.SESSION_NOT_FOUND, "세션이 없습니다: " + id));
        int size = Math.min(Math.max(limit, 1), 1000);
        List<SessionEvent> page;
        if (from != null || to != null) {
            page = events.findByTime(id, from == null ? Instant.EPOCH : from, to == null ? Instant.now() : to,
                    afterSeq, size);
        } else {
            page = events.findRange(id, afterSeq, toSeq == null ? session.lastSeq() : toSeq, size);
        }
        Long next = page.size() == size ? page.getLast().seq() : null;
        return new EventPage(page, next, session.lastSeq());
    }

    @Operation(summary = "스냅샷 생성/갱신", description = "head(또는 atSeq) 시점 스냅샷을 만든다. 같은 seq 가 이미 있으면 그대로 둔다(멱등).")
    @PostMapping("/snapshots")
    public ResponseEntity<SnapshotRepository.SnapshotInfo> createSnapshot(@PathVariable UUID id,
                                                                          @RequestParam(required = false) Long atSeq) {
        return ResponseEntity.status(HttpStatus.CREATED).body(restoreService.createSnapshot(id, atSeq));
    }

    @Operation(summary = "스냅샷 목록")
    @GetMapping("/snapshots")
    public List<SnapshotRepository.SnapshotInfo> listSnapshots(@PathVariable UUID id) {
        return snapshots.list(id);
    }
}
