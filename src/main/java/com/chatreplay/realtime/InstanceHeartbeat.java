package com.chatreplay.realtime;

import static com.chatreplay.common.Jdbc.ts;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.chatreplay.config.ChatProperties;
import com.chatreplay.config.InstanceId;
import com.chatreplay.event.AppendCommand;
import com.chatreplay.event.AppendResult;
import com.chatreplay.event.EventService;
import com.chatreplay.event.EventType;
import com.chatreplay.session.ParticipantRepository;
import com.chatreplay.session.ParticipantRow;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;

/**
 * 인스턴스 하트비트 + 장애 인스턴스 presence 정리(reaper).
 *
 * <p>인스턴스가 비정상 종료(kill -9, OOM, 노드 장애)되면 afterConnectionClosed 가 호출되지 않아
 * 참여자가 ONLINE 으로 남는다. 살아 있는 인스턴스의 reaper 가 하트비트가 끊긴 인스턴스의 연결을 찾아
 * DISCONNECT(reason=INSTANCE_DOWN)를 기록한다. 멱등 키가 "disconnect:{connectionId}" 로 같으므로
 * 여러 인스턴스의 reaper 가 동시에 돌아도 한 번만 기록된다.
 */
@Component
public class InstanceHeartbeat {

    private static final Logger log = LoggerFactory.getLogger(InstanceHeartbeat.class);

    private final JdbcClient jdbc;
    private final InstanceId instanceId;
    private final ParticipantRepository participants;
    private final EventService eventService;
    private final ConnectionRegistry registry;
    private final ChatProperties.Presence config;
    private final Clock clock;
    private final MeterRegistry meters;
    private final Instant startedAt;

    public InstanceHeartbeat(JdbcClient jdbc, InstanceId instanceId, ParticipantRepository participants,
                             EventService eventService, ConnectionRegistry registry, ChatProperties properties,
                             Clock clock, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.instanceId = instanceId;
        this.participants = participants;
        this.eventService = eventService;
        this.registry = registry;
        this.config = properties.presence();
        this.clock = clock;
        this.meters = meters;
        this.startedAt = clock.instant();
    }

    @Scheduled(fixedDelayString = "${chat.presence.heartbeat-interval}", initialDelay = 0)
    public void beat() {
        try {
            jdbc.sql("""
                            INSERT INTO server_instances (instance_id, started_at, heartbeat_at) VALUES (:id, :started, :now)
                            ON CONFLICT (instance_id) DO UPDATE SET heartbeat_at = EXCLUDED.heartbeat_at
                            """)
                    .param("id", instanceId.value())
                    .param("started", ts(startedAt))
                    .param("now", ts(clock.instant()))
                    .update();
        } catch (Exception e) {
            log.warn("heartbeat failed: {}", e.getMessage());
        }
    }

    @Scheduled(fixedDelayString = "${chat.presence.reaper-interval}", initialDelay = 5000)
    public void reap() {
        try {
            reapOnce();
        } catch (Exception e) {
            log.warn("presence reaper failed: {}", e.getMessage());
        }
    }

    /** @return 정리한 연결 수 */
    public int reapOnce() {
        Instant deadline = clock.instant().minus(config.instanceTimeout());
        List<ParticipantRow> stale = participants.findConnectionsOnDeadInstances(deadline, 200);
        int reaped = 0;
        for (ParticipantRow p : stale) {
            if (instanceId.value().equals(p.connectedInstance())) {
                continue; // 내 연결은 내가 안다(자기 자신을 죽은 인스턴스로 오판하지 않음)
            }
            AppendResult r;
            try {
                r = eventService.append(new AppendCommand(p.sessionId(), p.userId(), EventType.DISCONNECT,
                        "disconnect:" + p.connectionId(), Map.of("reason", "INSTANCE_DOWN"), null, p.connectionId(),
                        instanceId.value(), "system"));
            } catch (Exception e) {
                log.warn("failed to reap connection {}: {}", p.connectionId(), e.getMessage());
                continue; // 한 건 실패가 나머지 정리를 막지 않게
            }
            if (r.appended()) {
                reaped++;
                log.warn("reaped stale connection {} of {} on dead instance {}", p.connectionId(), p.userId(),
                        p.connectedInstance());
            }
        }
        if (reaped > 0) {
            meters.counter("chat.presence.reaped").increment(reaped);
        }
        jdbc.sql("DELETE FROM server_instances WHERE heartbeat_at < :old")
                .param("old", ts(clock.instant().minus(config.instanceTimeout().multipliedBy(30))))
                .update();
        return reaped;
    }

    /**
     * 정상 종료(배포/스케일 인): 연결을 1001(Going Away)로 닫아 클라이언트가 즉시 다른 인스턴스로 재연결하게 한다.
     * 닫힘 처리에서 DISCONNECT 가 기록되고, 클라이언트가 재연결하면 RECONNECT 가 기록된다.
     */
    @PreDestroy
    public void shutdown() {
        for (ClientConnection c : registry.all()) {
            c.close(CloseStatus.GOING_AWAY);
        }
        try {
            jdbc.sql("DELETE FROM server_instances WHERE instance_id = :id").param("id", instanceId.value()).update();
        } catch (Exception ignored) {
            // DB 가 먼저 내려갔으면 하트비트 만료로 정리된다
        }
    }
}
