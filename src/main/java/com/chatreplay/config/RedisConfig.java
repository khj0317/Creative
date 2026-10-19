package com.chatreplay.config;

import java.util.concurrent.Executors;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@Configuration
@ConditionalOnProperty(name = "chat.broadcast.mode", havingValue = "redis")
public class RedisConfig {

    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(RedisConnectionFactory factory) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(factory);
        // 기본값(SimpleAsyncTaskExecutor)은 메시지마다 스레드를 새로 만든다. 부하 시 비용이 커서 고정 풀로 바꾼다.
        // 순서는 연결의 재정렬 버퍼가 보장하므로 병렬 처리해도 된다.
        container.setTaskExecutor(Executors.newFixedThreadPool(8,
                Thread.ofPlatform().name("redis-listener-", 0).daemon().factory()));
        // Redis 연결이 끊기면 컨테이너가 재구독을 시도한다(기본 FixedBackOff). 끊긴 동안의 메시지는 resume 으로 복구.
        return container;
    }
}
