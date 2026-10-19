package com.chatreplay.session;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Size;

public final class SessionDtos {

    private SessionDtos() {
    }

    public record CreateSessionRequest(
            @Schema(example = "상담 세션") @Size(max = 200) String title,
            @Schema(description = "참여 가능한 사용자(최대 2명). 비우면 누구나 참여(선착순 2명).", example = "[\"alice\", \"bob\"]")
            @Size(max = 2) List<@Size(min = 1, max = 64) String> invitees) {
    }

    public record JoinRequest(
            @Schema(description = "멱등 키(선택). 생략하면 서버가 생성한다. 이미 참여 중이면 이벤트 없이 현재 상태를 돌려준다.")
            @Size(max = 80) String clientEventId) {
    }

    public record EndRequest(
            @Size(max = 80) String clientEventId,
            @Size(max = 200) String reason) {
    }

    public record ParticipantView(String userId, ParticipantState state, Presence presence, boolean online,
                                  Instant joinedAt, Instant leftAt, Instant lastSeenAt) {

        static ParticipantView of(ParticipantRow p) {
            return new ParticipantView(p.userId(), p.state(), p.presence(), p.presence() == Presence.ONLINE,
                    p.joinedAt(), p.leftAt(), p.lastSeenAt());
        }
    }

    @Schema(description = "비동기 프로젝션으로 계산한 요약. projectedSeq < headSeq 이면 아직 반영 중(최종적 일관성).")
    public record Summary(long projectedSeq, long lag, int messageCount, Long lastMessageSeq, Instant lastMessageAt,
                          String lastMessagePreview, long lastSnapshotSeq) {
    }

    public record SessionView(
            UUID id,
            String title,
            SessionStatus status,
            String createdBy,
            List<String> invitees,
            Instant createdAt,
            Instant endedAt,
            long headSeq,
            Instant lastEventAt,
            List<ParticipantView> participants,
            Summary summary) {
    }

    public record SessionPage(List<SessionView> items, String nextCursor) {
    }

    public record CommandResponse(String outcome, Long seq, SessionView session) {
    }
}
