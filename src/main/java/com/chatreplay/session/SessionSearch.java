package com.chatreplay.session;

import java.time.Instant;
import java.util.UUID;

public record SessionSearch(
        SessionStatus status,
        String participant,
        Instant from,
        Instant to,
        Instant cursorCreatedAt,
        UUID cursorId,
        int limit) {
}
