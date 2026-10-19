package com.chatreplay.replay;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.chatreplay.replay.SessionState.Message;
import com.chatreplay.replay.SessionState.MessageStatus;
import com.chatreplay.session.ParticipantState;
import com.chatreplay.session.Presence;
import com.chatreplay.session.SessionStatus;

/** GET /sessions/{id}/timeline 응답: 특정 시점의 세션 상태. */
public record TimelineResponse(
        UUID sessionId,
        Instant requestedAt,
        long seq,
        Instant stateAt,
        SessionStatus status,
        String title,
        String createdBy,
        Instant createdAt,
        Instant endedAt,
        List<ParticipantView> participants,
        int totalMessages,
        List<MessageView> messages,
        RestoreInfo restore) {

    public record ParticipantView(String userId, ParticipantState state, Presence presence, boolean online,
                                  Instant joinedAt, Instant leftAt, long lastReadSeq, int unreadCount) {
    }

    public record MessageView(long seq, String senderId, String content, MessageStatus status, Instant sentAt,
                              Instant editedAt, Instant deletedAt, int editCount) {
    }

    public record RestoreInfo(RestoreService.Strategy strategy, Long snapshotSeq, int replayedEvents,
                              long elapsedMicros) {
    }

    public static TimelineResponse of(RestoreService.Restored r, Instant requestedAt, int messageLimit) {
        SessionState s = r.state();
        List<Message> all = List.copyOf(s.messages().values());
        List<ParticipantView> participants = s.participants().values().stream()
                .map(p -> new ParticipantView(p.userId(), p.state(), p.presence(), p.presence() == Presence.ONLINE,
                        p.joinedAt(), p.leftAt(), p.lastReadSeq(), unread(all, p.userId(), p.lastReadSeq())))
                .toList();
        List<MessageView> recent = all.subList(Math.max(0, all.size() - messageLimit), all.size()).stream()
                .map(m -> new MessageView(m.seq(), m.senderId(), m.content(), m.status(), m.sentAt(), m.editedAt(),
                        m.deletedAt(), m.editCount()))
                .toList();
        return new TimelineResponse(s.sessionId(), requestedAt, s.seq(), s.at, s.status(), s.title, s.createdBy,
                s.createdAt, s.endedAt, participants, all.size(), recent,
                new RestoreInfo(r.strategy(), r.snapshotSeq(), r.replayedEvents(), r.elapsedMicros()));
    }

    private static int unread(List<Message> messages, String userId, long lastReadSeq) {
        int count = 0;
        for (Message m : messages) {
            if (m.seq() > lastReadSeq && !m.senderId().equals(userId) && m.status() != MessageStatus.DELETED) {
                count++;
            }
        }
        return count;
    }
}
