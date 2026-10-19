package com.chatreplay.session;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.chatreplay.common.ApiException;
import com.chatreplay.common.ErrorCode;
import com.chatreplay.event.AppendCommand;
import com.chatreplay.event.AppendResult;
import com.chatreplay.event.EventAppender;
import com.chatreplay.event.EventService;
import com.chatreplay.event.EventType;
import com.chatreplay.projection.ProjectionRepository;
import com.chatreplay.projection.ProjectionRepository.Projection;
import com.chatreplay.session.SessionDtos.CommandResponse;
import com.chatreplay.session.SessionDtos.CreateSessionRequest;
import com.chatreplay.session.SessionDtos.ParticipantView;
import com.chatreplay.session.SessionDtos.SessionPage;
import com.chatreplay.session.SessionDtos.SessionView;
import com.chatreplay.session.SessionDtos.Summary;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

@Service
public class SessionService {

    private final SessionRepository sessions;
    private final ParticipantRepository participants;
    private final ProjectionRepository projections;
    private final EventAppender appender;
    private final EventService eventService;
    private final Clock clock;

    public SessionService(SessionRepository sessions, ParticipantRepository participants,
                          ProjectionRepository projections, EventAppender appender, EventService eventService,
                          Clock clock) {
        this.sessions = sessions;
        this.participants = participants;
        this.projections = projections;
        this.appender = appender;
        this.eventService = eventService;
        this.clock = clock;
    }

    /** 세션 + SESSION_CREATED(seq=1) + 빈 프로젝션을 한 트랜잭션으로 만든다. */
    @Transactional
    public SessionView create(String userId, CreateSessionRequest req) {
        List<String> invitees = req == null || req.invitees() == null ? List.of()
                : List.copyOf(new LinkedHashSet<>(req.invitees()));
        if (invitees.size() > EventAppender.MAX_PARTICIPANTS) {
            throw new ApiException(ErrorCode.INVALID_REQUEST, "1:1 세션의 초대 인원은 최대 2명입니다.");
        }
        Instant now = clock.instant();
        SessionRow row = new SessionRow(UUID.randomUUID(), req == null ? null : req.title(), SessionStatus.WAITING,
                userId, invitees, now, null, 1, now);
        sessions.insert(row);
        appender.recordCreation(row);
        projections.insertEmpty(row.id(), now);
        return view(row, List.of(), null);
    }

    public CommandResponse join(UUID sessionId, String userId, String clientEventId) {
        String key = StringUtils.hasText(clientEventId) ? clientEventId : "join:" + UUID.randomUUID();
        AppendResult r = eventService.append(AppendCommand.rest(sessionId, userId, EventType.JOIN, key, Map.of(), null));
        return response(sessionId, r);
    }

    public CommandResponse leave(UUID sessionId, String userId, String clientEventId) {
        String key = StringUtils.hasText(clientEventId) ? clientEventId : "leave:" + UUID.randomUUID();
        AppendResult r = eventService.append(AppendCommand.rest(sessionId, userId, EventType.LEAVE, key, Map.of(), null));
        return response(sessionId, r);
    }

    /** 종료는 세션당 한 번뿐이므로 기본 멱등 키를 결정적으로 둔다(재시도해도 SESSION_ENDED 1건). */
    public CommandResponse end(UUID sessionId, String userId, String clientEventId, String reason) {
        String key = StringUtils.hasText(clientEventId) ? clientEventId : "end:" + sessionId;
        AppendResult r;
        try {
            r = eventService.append(AppendCommand.rest(sessionId, userId, EventType.SESSION_ENDED, key,
                    reason == null ? Map.of() : Map.of("reason", reason), null));
        } catch (ApiException e) {
            if (e.code() != ErrorCode.SESSION_ENDED) {
                throw e;
            }
            r = AppendResult.skipped(0, SessionStatus.COMPLETED, "already ended");
        }
        return response(sessionId, r);
    }

    public SessionView get(UUID sessionId) {
        SessionRow row = sessions.findById(sessionId)
                .orElseThrow(() -> new ApiException(ErrorCode.SESSION_NOT_FOUND, "세션이 없습니다: " + sessionId));
        return view(row, participants.findBySession(sessionId), projections.find(sessionId).orElse(null));
    }

    public SessionPage list(SessionStatus status, String participant, Instant from, Instant to, String cursor,
                            int limit) {
        int size = Math.min(Math.max(limit, 1), 100);
        Instant cursorAt = null;
        UUID cursorId = null;
        if (StringUtils.hasText(cursor)) {
            try {
                String[] parts = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8).split("\\|");
                cursorAt = Instant.parse(parts[0]);
                cursorId = UUID.fromString(parts[1]);
            } catch (RuntimeException e) {
                throw new ApiException(ErrorCode.INVALID_REQUEST, "cursor 형식이 잘못되었습니다.");
            }
        }
        List<SessionRow> rows = sessions.search(new SessionSearch(status, participant, from, to, cursorAt, cursorId,
                size + 1));
        boolean hasMore = rows.size() > size;
        List<SessionRow> page = hasMore ? rows.subList(0, size) : rows;

        // N+1 방지: 페이지의 참여자/프로젝션을 IN 쿼리 2번으로 가져온다.
        List<UUID> ids = page.stream().map(SessionRow::id).toList();
        Map<UUID, List<ParticipantRow>> byId = participants.findBySessions(ids).stream()
                .collect(Collectors.groupingBy(ParticipantRow::sessionId));
        Map<UUID, Projection> projById = projections.findAll(ids).stream()
                .collect(Collectors.toMap(Projection::sessionId, Function.identity()));
        List<SessionView> items = page.stream()
                .map(s -> view(s, byId.getOrDefault(s.id(), List.of()), projById.get(s.id())))
                .toList();
        String next = null;
        if (hasMore) {
            SessionRow last = page.getLast();
            next = Base64.getUrlEncoder().withoutPadding()
                    .encodeToString((last.createdAt() + "|" + last.id()).getBytes(StandardCharsets.UTF_8));
        }
        return new SessionPage(items, next);
    }

    private CommandResponse response(UUID sessionId, AppendResult r) {
        return new CommandResponse(r.outcome().name(), r.event() == null ? null : r.event().seq(), get(sessionId));
    }

    private static SessionView view(SessionRow s, List<ParticipantRow> ps, Projection p) {
        Summary summary = p == null ? null : new Summary(p.projectedSeq(), Math.max(0, s.lastSeq() - p.projectedSeq()),
                p.messageCount(), p.lastMessageSeq(), p.lastMessageAt(), p.lastMessagePreview(), p.lastSnapshotSeq());
        return new SessionView(s.id(), s.title(), s.status(), s.createdBy(), s.invitees(), s.createdAt(), s.endedAt(),
                s.lastSeq(), s.lastEventAt(), ps.stream().map(ParticipantView::of).toList(), summary);
    }
}
