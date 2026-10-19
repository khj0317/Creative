package com.chatreplay.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "chat")
public record ChatProperties(
        String instanceId,
        Broadcast broadcast,
        Presence presence,
        Projection projection,
        Snapshot snapshot,
        Replay replay) {

    public enum BroadcastMode { LOCAL, REDIS }

    public record Broadcast(BroadcastMode mode) {
    }

    public record Presence(Duration heartbeatInterval, Duration instanceTimeout, Duration reaperInterval) {
    }

    public record Projection(Duration pollInterval, int batchSize, int eventsPerRun, Duration lease,
                             int maxAttempts, Duration backoffBase, Duration backoffMax) {
    }

    public record Snapshot(int interval) {
    }

    public record Replay(int pageSize, int defaultMessageLimit) {
    }
}
