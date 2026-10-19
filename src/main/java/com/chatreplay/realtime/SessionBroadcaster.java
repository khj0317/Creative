package com.chatreplay.realtime;

/**
 * 커밋된 이벤트를 "해당 세션에 연결된 모든 인스턴스" 로 퍼뜨린다.
 * local: 단일 인스턴스(개발/테스트), redis: Redis Pub/Sub 로 수평 확장.
 *
 * <p>전달 보장은 at-most-once 다. 유실돼도 이벤트는 DB 에 있고 seq 가 빈틈없이 증가하므로
 * 연결 측에서 빈틈을 감지해 DB 에서 메운다(→ 클라이언트 입장에서는 순서 보장 + 유실 없음).
 */
public interface SessionBroadcaster {

    void publish(BroadcastEnvelope envelope);
}
