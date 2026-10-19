package com.chatreplay.event;

import com.chatreplay.session.SessionStatus;

/**
 * @param outcome APPENDED: 새로 저장됨 / DUPLICATE: 같은 멱등 키로 이미 저장됨(기존 이벤트 반환)
 *                / SKIPPED: 상태 변화가 없어 이벤트를 남기지 않음(예: 이미 참여 중인데 다시 JOIN)
 * @param event   APPENDED/DUPLICATE 일 때 해당 이벤트, SKIPPED 이면 null
 */
public record AppendResult(Outcome outcome, SessionEvent event, long headSeq, SessionStatus status, String reason) {

    public enum Outcome { APPENDED, DUPLICATE, SKIPPED }

    public static AppendResult appended(SessionEvent event, SessionStatus status) {
        return new AppendResult(Outcome.APPENDED, event, event.seq(), status, null);
    }

    public static AppendResult duplicate(SessionEvent event, long headSeq, SessionStatus status) {
        return new AppendResult(Outcome.DUPLICATE, event, headSeq, status, "duplicate clientEventId");
    }

    public static AppendResult skipped(long headSeq, SessionStatus status, String reason) {
        return new AppendResult(Outcome.SKIPPED, null, headSeq, status, reason);
    }

    public boolean appended() {
        return outcome == Outcome.APPENDED;
    }
}
