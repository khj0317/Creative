package com.chatreplay.config;

import java.net.InetAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

@Configuration
public class AppConfig {

    /** PostgreSQL timestamptz 정밀도(마이크로초)에 맞춰, 응답으로 준 시각과 저장된 시각이 정확히 같게 한다. */
    @Bean
    public Clock clock() {
        return Clock.tick(Clock.systemUTC(), Duration.ofNanos(1_000));
    }

    @Bean
    public InstanceId instanceId(ChatProperties properties) {
        if (StringUtils.hasText(properties.instanceId())) {
            return new InstanceId(properties.instanceId());
        }
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown";
        }
        return new InstanceId(host + "-" + UUID.randomUUID().toString().substring(0, 8));
    }

    @Bean
    public OpenAPI openApi() {
        return new OpenAPI().info(new Info()
                .title("Realtime Session Replay API")
                .version("0.1.0")
                .description("""
                        1:1 실시간 채팅 + 이벤트 소싱 기반 특정 시점 상태 복원 API.
                        인증은 과제 범위 밖이므로 X-User-Id 헤더로 사용자를 식별한다.
                        실시간 통신은 WebSocket `/ws/sessions/{id}?userId=&lastSeq=` 를 사용한다(docs/websocket-protocol.md).
                        """));
    }
}
