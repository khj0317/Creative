package com.chatreplay.replay;

import java.util.List;

import com.chatreplay.event.SessionEvent;
import com.chatreplay.replay.SessionState.Message;
import com.chatreplay.replay.SessionState.MessageStatus;
import com.chatreplay.replay.SessionState.Participant;
import com.chatreplay.session.ParticipantState;
import com.chatreplay.session.Presence;
import com.chatreplay.session.SessionStatus;

/**
 * 순수 함수형 리듀서: state' = apply(state, event).
 * 같은 이벤트 열(seq 순)이면 언제, 어느 인스턴스에서, 스냅샷을 쓰든 안 쓰든 항상 같은 상태가 나온다.
 *
 * <p>중복/순서 방어(복원 로직 측):
 * <ul>
 *   <li>seq &lt;= state.seq 인 이벤트는 이미 반영된 것으로 보고 무시한다(재적용 멱등).</li>
 *   <li>seq 에 빈틈이 있으면 저장소 손상으로 보고 예외를 던진다(조용히 틀린 상태를 만들지 않음).</li>
 *   <li>존재하지 않거나 삭제된 메시지에 대한 수정/삭제는 no-op 이다(쓰기 경로에서 이미 거절되지만 이중 방어).</li>
 * </ul>
 */
public final class SessionStateReducer {

    private SessionStateReducer() {
    }

    public static void applyAll(SessionState s, List<SessionEvent> events) {
        for (SessionEvent e : events) {
            apply(s, e);
        }
    }

    public static void apply(SessionState s, SessionEvent e) {
        if (e.seq() <= s.seq) {
            return;
        }
        if (e.seq() != s.seq + 1) {
            throw new IllegalStateException("event gap detected: state.seq=" + s.seq + ", next=" + e.seq()
                    + " (session " + s.sessionId + ")");
        }
        switch (e.type()) {
            case SESSION_CREATED -> {
                s.title = e.stringPayload("title");
                s.createdBy = e.actorId();
                s.createdAt = e.serverTs();
            }
            case JOIN -> {
                Participant prev = s.participants.get(e.actorId());
                Presence presence = "ws".equals(e.stringPayload("via")) ? Presence.ONLINE : Presence.OFFLINE;
                s.participants.put(e.actorId(), new Participant(e.actorId(), ParticipantState.JOINED, presence,
                        e.serverTs(), null, prev == null ? 0 : prev.lastReadSeq()));
            }
            case LEAVE -> s.participants.computeIfPresent(e.actorId(), (k, p) -> new Participant(
                    p.userId(), ParticipantState.LEFT, Presence.OFFLINE, p.joinedAt(), e.serverTs(), p.lastReadSeq()));
            case DISCONNECT -> s.participants.computeIfPresent(e.actorId(), (k, p) -> new Participant(
                    p.userId(), p.state(), Presence.DISCONNECTED, p.joinedAt(), p.leftAt(), p.lastReadSeq()));
            case RECONNECT -> s.participants.computeIfPresent(e.actorId(), (k, p) -> new Participant(
                    p.userId(), p.state(), Presence.ONLINE, p.joinedAt(), p.leftAt(), p.lastReadSeq()));
            case MESSAGE -> s.messages.put(e.seq(), new Message(e.seq(), e.actorId(), e.stringPayload("content"),
                    MessageStatus.SENT, e.serverTs(), null, null, 0));
            case MESSAGE_EDITED -> {
                long target = e.longPayload("targetSeq");
                Message m = s.messages.get(target);
                if (m != null && m.status() != MessageStatus.DELETED && m.senderId().equals(e.actorId())) {
                    s.messages.put(target, new Message(m.seq(), m.senderId(), e.stringPayload("content"),
                            MessageStatus.EDITED, m.sentAt(), e.serverTs(), null, m.editCount() + 1));
                }
            }
            case MESSAGE_DELETED -> {
                long target = e.longPayload("targetSeq");
                Message m = s.messages.get(target);
                if (m != null && m.status() != MessageStatus.DELETED && m.senderId().equals(e.actorId())) {
                    s.messages.put(target, new Message(m.seq(), m.senderId(), null, MessageStatus.DELETED,
                            m.sentAt(), m.editedAt(), e.serverTs(), m.editCount()));
                }
            }
            case READ -> {
                long upTo = e.longPayload("upToSeq");
                // 읽음 위치는 max 로만 움직인다: 늦게 도착한 오래된 READ 가 와도 되돌아가지 않는다(순서 무관).
                s.participants.computeIfPresent(e.actorId(), (k, p) -> new Participant(p.userId(), p.state(),
                        p.presence(), p.joinedAt(), p.leftAt(), Math.max(p.lastReadSeq(), upTo)));
            }
            case SESSION_ENDED -> s.endedAt = e.serverTs();
        }
        s.seq = e.seq();
        s.at = e.serverTs();
        s.appliedEvents++;
        s.status = SessionStatus.derive(s.endedAt != null, s.participants.values().stream()
                .map(p -> new SessionStatus.ParticipantStatus(p.state(), p.presence()))
                .toList());
    }
}
