package com.chatreplay.session;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record SessionRow(
        UUID id,
        String title,
        SessionStatus status,
        String createdBy,
        List<String> invitees,
        Instant createdAt,
        Instant endedAt,
        long lastSeq,
        Instant lastEventAt) {

    public boolean ended() {
        return status == SessionStatus.COMPLETED;
    }
}
