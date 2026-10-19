package com.chatreplay.session;

import java.time.Instant;
import java.util.UUID;

public record ParticipantRow(
        UUID sessionId,
        String userId,
        ParticipantState state,
        Presence presence,
        Instant joinedAt,
        Instant leftAt,
        Instant lastSeenAt,
        String connectionId,
        String connectedInstance) {

    public boolean joined() {
        return state == ParticipantState.JOINED;
    }

    public SessionStatus.ParticipantStatus status() {
        return new SessionStatus.ParticipantStatus(state, presence);
    }
}
