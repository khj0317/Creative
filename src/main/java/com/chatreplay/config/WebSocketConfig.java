package com.chatreplay.config;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.chatreplay.realtime.ChatWebSocketHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final ChatWebSocketHandler handler;

    public WebSocketConfig(ChatWebSocketHandler handler) {
        this.handler = handler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        // 인증은 과제 범위 밖이라 Origin 제한도 두지 않는다(운영에서는 토큰 검증 HandshakeInterceptor + 허용 Origin 지정).
        registry.addHandler(handler, "/ws/sessions/*").setAllowedOrigins("*");
    }

    /**
     * 실시간 전달 전용 풀(플랫폼 스레드). 전송은 블로킹 소켓 쓰기이고 JDK 21 가상 스레드는 synchronized 구간에서
     * 캐리어 스레드에 고정(pinning)될 수 있어, 전달처럼 블로킹 I/O 가 많은 경로는 크기가 정해진 플랫폼 풀로 돌린다.
     * 재정렬 대기 후 재시도를 위해 ScheduledExecutorService 를 쓴다.
     */
    @Bean(destroyMethod = "shutdown")
    public static ScheduledExecutorService deliveryExecutor() {
        int threads = Math.max(8, Runtime.getRuntime().availableProcessors() * 4);
        return Executors.newScheduledThreadPool(threads, Thread.ofPlatform().name("ws-delivery-", 0).daemon().factory());
    }
}
