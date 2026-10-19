package com.chatreplay.session;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.chatreplay.event.EventService;
import com.chatreplay.event.EventService.ItemResult;
import com.chatreplay.event.EventRequest;
import com.chatreplay.session.SessionDtos.CommandResponse;
import com.chatreplay.session.SessionDtos.CreateSessionRequest;
import com.chatreplay.session.SessionDtos.EndRequest;
import com.chatreplay.session.SessionDtos.JoinRequest;
import com.chatreplay.session.SessionDtos.SessionPage;
import com.chatreplay.session.SessionDtos.SessionView;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import org.slf4j.MDC;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "Sessions", description = "세션 생성/참여/종료/조회 및 이벤트 수집")
@RestController
@RequestMapping("/sessions")
public class SessionController {

    public static final String USER_HEADER = "X-User-Id";

    private final SessionService sessionService;
    private final EventService eventService;

    public SessionController(SessionService sessionService, EventService eventService) {
        this.sessionService = sessionService;
        this.eventService = eventService;
    }

    @Operation(summary = "세션 생성", description = "SESSION_CREATED(seq=1) 이벤트와 함께 생성된다.")
    @PostMapping
    public ResponseEntity<SessionView> create(@RequestHeader(USER_HEADER) String userId,
                                              @Valid @RequestBody(required = false) CreateSessionRequest body) {
        return ResponseEntity.status(HttpStatus.CREATED).body(sessionService.create(userId, body));
    }

    @Operation(summary = "세션 목록", description = "상태/참여자/생성 기간 필터 + 키셋 커서 페이지네이션(created_at DESC).")
    @GetMapping
    public SessionPage list(
            @RequestParam(required = false) SessionStatus status,
            @RequestParam(required = false) String participant,
            @Parameter(description = "생성 시각 하한(포함), ISO-8601")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @Parameter(description = "생성 시각 상한(미포함), ISO-8601")
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(required = false) String cursor,
            @RequestParam(defaultValue = "20") int limit) {
        return sessionService.list(status, participant, from, to, cursor, limit);
    }

    @Operation(summary = "세션 상세", description = "참여자/presence 는 동기(강한 일관성), summary 는 비동기 프로젝션(최종적 일관성).")
    @GetMapping("/{id}")
    public SessionView get(@PathVariable UUID id) {
        return sessionService.get(id);
    }

    @Operation(summary = "세션 참여", description = "이미 참여 중이면 이벤트 없이 SKIPPED. 1:1 이므로 최대 2명.")
    @PostMapping("/{id}/join")
    public CommandResponse join(@PathVariable UUID id, @RequestHeader(USER_HEADER) String userId,
                                @Valid @RequestBody(required = false) JoinRequest body) {
        MDC.put("sessionId", id.toString());
        return sessionService.join(id, userId, body == null ? null : body.clientEventId());
    }

    @Operation(summary = "세션 퇴장")
    @PostMapping("/{id}/leave")
    public CommandResponse leave(@PathVariable UUID id, @RequestHeader(USER_HEADER) String userId,
                                 @Valid @RequestBody(required = false) JoinRequest body) {
        MDC.put("sessionId", id.toString());
        return sessionService.leave(id, userId, body == null ? null : body.clientEventId());
    }

    @Operation(summary = "세션 종료", description = "멱등: 이미 종료된 세션이면 SKIPPED.")
    @PostMapping("/{id}/end")
    public CommandResponse end(@PathVariable UUID id, @RequestHeader(USER_HEADER) String userId,
                               @Valid @RequestBody(required = false) EndRequest body) {
        MDC.put("sessionId", id.toString());
        return sessionService.end(id, userId, body == null ? null : body.clientEventId(),
                body == null ? null : body.reason());
    }

    public record IngestRequest(@NotEmpty @Size(max = 100) List<@Valid EventRequest> events) {
    }

    public record IngestResponse(List<ItemResult> results) {
    }

    @Operation(summary = "이벤트/메시지 수집(배치)",
            description = """
                    항목마다 독립적으로 처리되며 결과도 항목별로 돌려준다(CREATED/DUPLICATE/SKIPPED/REJECTED/FAILED).
                    같은 clientEventId 재전송은 저장되지 않고 처음 부여된 seq 를 DUPLICATE 로 돌려준다.
                    순서 기준은 서버가 부여하는 seq 이며, 배열 순서대로 처리된다.""")
    @PostMapping("/{id}/events")
    public IngestResponse ingest(@PathVariable UUID id, @RequestHeader(USER_HEADER) String userId,
                                 @Valid @RequestBody IngestRequest body) {
        MDC.put("sessionId", id.toString());
        return new IngestResponse(eventService.ingest(id, userId, body.events()));
    }
}
