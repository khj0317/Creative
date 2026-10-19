package com.chatreplay.projection;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.chatreplay.common.ApiException;
import com.chatreplay.common.ErrorCode;
import com.chatreplay.realtime.InstanceHeartbeat;
import com.chatreplay.session.SessionRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 운영용 엔드포인트(운영 환경에서는 내부망/관리자 인증 뒤에 둔다). */
@Tag(name = "Admin", description = "DLQ 재처리, 프로젝션 재구축, 장애 주입(로컬 전용)")
@RestController
@RequestMapping("/admin")
public class AdminController {

    private final ProjectionOutboxRepository outbox;
    private final ProjectionRepository projections;
    private final ProjectionFaultInjector faults;
    private final SessionRepository sessions;
    private final InstanceHeartbeat heartbeat;
    private final Clock clock;
    private final boolean faultInjectionEnabled;

    public AdminController(ProjectionOutboxRepository outbox, ProjectionRepository projections,
                           ProjectionFaultInjector faults, SessionRepository sessions, InstanceHeartbeat heartbeat, Clock clock,
                           @Value("${chat.fault-injection.enabled:false}") boolean faultInjectionEnabled) {
        this.outbox = outbox;
        this.projections = projections;
        this.faults = faults;
        this.sessions = sessions;
        this.heartbeat = heartbeat;
        this.clock = clock;
        this.faultInjectionEnabled = faultInjectionEnabled;
    }

    @Operation(summary = "프로젝션 DLQ 목록(미해결)")
    @GetMapping("/projections/dlq")
    public List<ProjectionOutboxRepository.DlqEntry> dlq() {
        return outbox.findUnresolvedDlq(100);
    }

    @Operation(summary = "DLQ 항목 재처리", description = "원인 해결 후 호출. 해결 표시 + outbox 재적재(체크포인트 멱등이라 안전).")
    @PostMapping("/projections/dlq/{dlqId}/redrive")
    @Transactional
    public Map<String, Object> redrive(@PathVariable long dlqId) {
        return Map.of("redriven", outbox.redrive(dlqId, clock.instant()));
    }

    @Operation(summary = "프로젝션 재구축", description = "체크포인트를 0으로 되돌리고 다시 계산한다(정합성 의심 시). 이벤트는 그대로이므로 결과는 결정적.")
    @PostMapping("/sessions/{sessionId}/projection/rebuild")
    @Transactional
    public Map<String, Object> rebuild(@PathVariable UUID sessionId) {
        long head = sessions.findById(sessionId)
                .orElseThrow(() -> new ApiException(ErrorCode.SESSION_NOT_FOUND, "세션이 없습니다: " + sessionId))
                .lastSeq();
        projections.reset(sessionId, clock.instant());
        outbox.enqueue(sessionId, head, clock.instant());
        return Map.of("rebuildQueued", true);
    }

    @Operation(summary = "장애 인스턴스 presence 정리 즉시 실행")
    @PostMapping("/presence/reap")
    public Map<String, Object> reap() {
        return Map.of("reaped", heartbeat.reapOnce());
    }

    @Operation(summary = "[장애 주입] 세션 프로젝션 강제 실패 on/off", description = "chat.fault-injection.enabled=true 일 때만 동작.")
    @PostMapping("/faults/projection/{sessionId}")
    public Map<String, Object> projectionFault(@PathVariable UUID sessionId, @RequestParam boolean enabled) {
        if (!faultInjectionEnabled) {
            throw new ApiException(ErrorCode.INVALID_REQUEST, "fault injection is disabled");
        }
        if (enabled) {
            faults.failSession(sessionId);
        } else {
            faults.heal(sessionId);
        }
        return Map.of("sessionId", sessionId, "failing", enabled);
    }
}
