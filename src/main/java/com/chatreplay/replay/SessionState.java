package com.chatreplay.replay;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import com.chatreplay.session.ParticipantState;
import com.chatreplay.session.Presence;
import com.chatreplay.session.SessionStatus;

/**
 * 리플레이 중인 세션 상태(가변). 결정성을 위해
 * <ul>
 *   <li>컬렉션은 정렬된 TreeMap(참여자: userId, 메시지: seq)만 쓰고</li>
 *   <li>시각은 이벤트의 server_ts 만 쓰며(now() 금지)</li>
 *   <li>스냅샷 직렬화 형식({@link Data})은 버전을 둔다.</li>
 * </ul>
 */
public final class SessionState {

    /** 스냅샷 상태 스키마 버전. 형식이 바뀌면 올리고, 다른 버전의 스냅샷은 무시하고 전체 리플레이한다. */
    public static final int VERSION = 1;

    public enum MessageStatus { SENT, EDITED, DELETED }

    public record Participant(String userId, ParticipantState state, Presence presence, Instant joinedAt,
                              Instant leftAt, long lastReadSeq) {
    }

    public record Message(long seq, String senderId, String content, MessageStatus status, Instant sentAt,
                          Instant editedAt, Instant deletedAt, int editCount) {
    }

    /** 스냅샷(JSONB)으로 저장되는 불변 표현 */
    public record Data(UUID sessionId, long seq, Instant at, SessionStatus status, String title, String createdBy,
                       Instant createdAt, Instant endedAt, long appliedEvents,
                       List<Participant> participants, List<Message> messages) {
    }

    final UUID sessionId;
    long seq;
    Instant at;
    SessionStatus status = SessionStatus.WAITING;
    String title;
    String createdBy;
    Instant createdAt;
    Instant endedAt;
    long appliedEvents;
    final TreeMap<String, Participant> participants = new TreeMap<>();
    final TreeMap<Long, Message> messages = new TreeMap<>();

    public SessionState(UUID sessionId) {
        this.sessionId = sessionId;
    }

    public static SessionState from(Data d) {
        SessionState s = new SessionState(d.sessionId());
        s.seq = d.seq();
        s.at = d.at();
        s.status = d.status();
        s.title = d.title();
        s.createdBy = d.createdBy();
        s.createdAt = d.createdAt();
        s.endedAt = d.endedAt();
        s.appliedEvents = d.appliedEvents();
        d.participants().forEach(p -> s.participants.put(p.userId(), p));
        d.messages().forEach(m -> s.messages.put(m.seq(), m));
        return s;
    }

    public Data toData() {
        return new Data(sessionId, seq, at, status, title, createdBy, createdAt, endedAt, appliedEvents,
                new ArrayList<>(participants.values()), new ArrayList<>(messages.values()));
    }

    public UUID sessionId() {
        return sessionId;
    }

    public long seq() {
        return seq;
    }

    public SessionStatus status() {
        return status;
    }

    public Map<String, Participant> participants() {
        return participants;
    }

    public Map<Long, Message> messages() {
        return messages;
    }
}
