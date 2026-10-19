package com.chatreplay.session;

import java.util.Collection;

/**
 * 세션 상태. 과제의 "진행중/중단/완료" 에 대기(WAITING)를 더했다.
 * 상태는 저장된 사실(참여자 상태 + 종료 여부)에서 항상 같은 규칙으로 계산한다.
 * 쓰기 모델(sessions.status)과 이벤트 리플레이(SessionStateReducer)가 이 규칙 하나를 공유하므로
 * "현재 상태" 와 "head 시점까지 리플레이한 상태" 가 어긋나지 않는다.
 */
public enum SessionStatus {
    /** 참여 중인 사람이 없음(생성 직후 또는 모두 퇴장) */
    WAITING,
    /** 진행중: 참여자가 있고 비정상 단절된 참여자가 없음 */
    ACTIVE,
    /** 중단: 참여 중인 누군가의 연결이 끊김(재연결 대기) */
    INTERRUPTED,
    /** 완료: 종료됨(terminal) */
    COMPLETED;

    public record ParticipantStatus(ParticipantState state, Presence presence) {
    }

    public static SessionStatus derive(boolean ended, Collection<ParticipantStatus> participants) {
        if (ended) {
            return COMPLETED;
        }
        boolean anyJoined = false;
        for (ParticipantStatus p : participants) {
            if (p.state() != ParticipantState.JOINED) {
                continue;
            }
            anyJoined = true;
            if (p.presence() == Presence.DISCONNECTED) {
                return INTERRUPTED;
            }
        }
        return anyJoined ? ACTIVE : WAITING;
    }
}
