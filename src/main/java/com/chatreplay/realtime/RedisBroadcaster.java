package com.chatreplay.realtime;

import java.nio.charset.StandardCharsets;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * 수평 확장용: 세션별 채널(chat:session:{id})로 발행하고, 각 인스턴스는 기동 시 패턴(chat:session:*)을 한 번 구독한다.
 * 수신한 메시지는 로컬 연결이 있는 세션만 처리한다(LocalDispatcher).
 *
 * <p>처음에는 "로컬 연결이 생길 때 세션 채널을 동적 구독" 했으나, 부하 테스트에서 동시 구독 요청이 겹치면
 * 구독이 누락/중복되어 실시간 전달이 안전망 주기로 밀리는 현상이 재현되어 패턴 구독으로 바꿨다.
 * 대가는 모든 인스턴스가 모든 세션 메시지를 받는 것(인스턴스 수 N 에 비례). 수십 대 이상이면
 * Redis 7 Sharded Pub/Sub(SSUBSCRIBE) + 세션→인스턴스 라우팅으로 확장한다(docs/design.md).
 *
 * <p>Pub/Sub 은 유실될 수 있으나(구독 직전/Redis 재시작), 연결 측 seq 빈틈 메우기 + 클라이언트 resume 으로 복구된다.
 */
@Component
@ConditionalOnProperty(name = "chat.broadcast.mode", havingValue = "redis")
public class RedisBroadcaster implements SessionBroadcaster, MessageListener {

    private static final Logger log = LoggerFactory.getLogger(RedisBroadcaster.class);
    private static final String PREFIX = "chat:session:";

    private final StringRedisTemplate redis;
    private final RedisMessageListenerContainer container;
    private final LocalDispatcher dispatcher;
    private final JsonMapper json;
    private final MeterRegistry meters;

    public RedisBroadcaster(StringRedisTemplate redis, RedisMessageListenerContainer container,
                            LocalDispatcher dispatcher, JsonMapper json, MeterRegistry meters) {
        this.redis = redis;
        this.container = container;
        this.dispatcher = dispatcher;
        this.json = json;
        this.meters = meters;
    }

    @Override
    public void publish(BroadcastEnvelope envelope) {
        try {
            redis.convertAndSend(PREFIX + envelope.sessionId(), json.writeValueAsString(envelope));
        } catch (Exception e) {
            // Redis 장애: 이벤트는 이미 DB 에 커밋됨. 로컬 연결에는 직접 전달하고, 다른 인스턴스의 클라이언트는
            // ping 응답(headSeq)으로 뒤처짐을 감지해 resume 한다.
            meters.counter("chat.broadcast.failures").increment();
            log.warn("redis publish failed, falling back to local dispatch: {}", e.getMessage());
            dispatcher.dispatch(envelope);
        }
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            String body = new String(message.getBody(), StandardCharsets.UTF_8);
            dispatcher.dispatch(json.readValue(body, BroadcastEnvelope.class));
        } catch (Exception e) {
            log.warn("invalid broadcast message: {}", e.getMessage());
        }
    }

    @PostConstruct
    void subscribe() {
        container.addMessageListener(this, new PatternTopic(PREFIX + "*"));
    }
}
