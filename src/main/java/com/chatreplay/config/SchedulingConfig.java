package com.chatreplay.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** 워커/하트비트/reaper 스케줄링. 테스트에서는 끄고(chat.scheduling.enabled=false) 직접 호출해 결정적으로 검증한다. */
@Configuration
@EnableScheduling
@ConditionalOnProperty(name = "chat.scheduling.enabled", havingValue = "true", matchIfMissing = true)
public class SchedulingConfig {
}
