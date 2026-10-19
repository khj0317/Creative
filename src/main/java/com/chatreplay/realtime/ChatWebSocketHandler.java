package com.chatreplay.realtime;

import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import com.chatreplay.common.ApiException;
import com.chatreplay.config.InstanceId;
import com.chatreplay.event.AppendCommand;
import com.chatreplay.event.AppendResult;
import com.chatreplay.event.EventRepository;
import com.chatreplay.event.EventService;
import com.chatreplay.event.EventType;
import com.chatreplay.session.SessionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriComponentsBuilder;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * WebSocket: /ws/sessions/{sessionId}?userId=...&lastSeq=...
 *
 * <pre>
 * 연결 수립: 구독 등록 → JOIN 또는 RECONNECT 기록 → welcome → lastSeq 이후 이벤트 resume → 실시간
 * client → server : send / typing / resume / ping
 * server → client : welcome / event / ack / error / typing / pong
 * 연결 종료: (현재 연결일 때만) DISCONNECT 기록
 * </pre>
 * 자세한 프로토콜은 docs/websocket-protocol.md 참고.
 */
@Component
public class ChatWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(ChatWebSocketHandler.class);
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };
    private static final String ATTR_CONN = "conn";

    private final ConnectionRegistry registry;
    private final SessionBroadcaster broadcaster;
    private final EventService eventService;
    private final EventRepository events;
    private final SessionRepository sessions;
    private final InstanceId instanceId;
    private final JsonMapper json;
    private final ScheduledExecutorService deliveryExecutor;

    public ChatWebSocketHandler(ConnectionRegistry registry, SessionBroadcaster broadcaster,
                                EventService eventService, EventRepository events, SessionRepository sessions,
                                InstanceId instanceId, JsonMapper json,
                                @Qualifier("deliveryExecutor")
                                ScheduledExecutorService deliveryExecutor) {
        this.registry = registry;
        this.broadcaster = broadcaster;
        this.eventService = eventService;
        this.events = events;
        this.sessions = sessions;
        this.instanceId = instanceId;
        this.json = json;
        this.deliveryExecutor = deliveryExecutor;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession raw) throws Exception {
        WebSocketSession ws = new ConcurrentWebSocketSessionDecorator(raw, 10_000, 512 * 1024);
        Handshake hs;
        try {
            hs = Handshake.parse(raw.getUri());
        } catch (IllegalArgumentException e) {
            sendRaw(ws, Map.of("type", "error", "code", "INVALID_REQUEST", "message", e.getMessage()));
            ws.close(CloseStatus.POLICY_VIOLATION);
            return;
        }
        String connectionId = UUID.randomUUID().toString();
        // lastSeq 가 없으면 "지금" 부터: 연결 시점의 head 이후 이벤트(자신의 JOIN/RECONNECT 포함)를 받는다.
        long startSeq = hs.lastSeq() != null ? hs.lastSeq()
                : sessions.findById(hs.sessionId()).map(s -> s.lastSeq()).orElse(0L);
        ClientConnection conn = new ClientConnection(ws, hs.sessionId(), hs.userId(), connectionId, startSeq,
                events, json, deliveryExecutor);
        raw.getAttributes().put(ATTR_CONN, conn);
        withMdc(conn, () -> {
            conn.holdDelivery();
            try {
                establish(conn, hs);
            } finally {
                conn.releaseDelivery();
            }
        });
    }

    private void establish(ClientConnection conn, Handshake hs) {
        // 1) 구독을 먼저 등록해 연결 수립 도중 발생한 이벤트도 놓치지 않는다(중복은 seq 로 제거).
        registry.add(conn);
        // 2) 참여/재연결 기록. 결정적 멱등 키(connect:{connectionId})라 재시도해도 한 번만 기록된다.
        Map<String, Object> payload = new HashMap<>();
        if (hs.lastSeq() != null) {
            payload.put("resumeFromSeq", hs.lastSeq());
        }
        AppendResult result;
        try {
            result = eventService.append(new AppendCommand(hs.sessionId(), hs.userId(), EventType.RECONNECT,
                    "connect:" + conn.connectionId(), payload, null, conn.connectionId(), instanceId.value(), "ws"));
        } catch (ApiException e) {
            conn.send(Map.of("type", "error", "code", e.code().name(), "message", e.getMessage()));
            conn.markReplaced(); // DISCONNECT 를 남기지 않도록
            conn.close(new CloseStatus(4000 + e.code().status().value() % 1000, e.code().name()));
            return;
        }
        // 3) 다른 인스턴스/탭에 남아 있는 같은 사용자의 이전 연결 정리
        broadcaster.publish(BroadcastEnvelope.replaced(hs.sessionId(), hs.userId(), conn.connectionId(),
                instanceId.value()));

        Map<String, Object> welcome = new LinkedHashMap<>();
        welcome.put("type", "welcome");
        welcome.put("sessionId", hs.sessionId());
        welcome.put("userId", hs.userId());
        welcome.put("connectionId", conn.connectionId());
        welcome.put("headSeq", result.headSeq());
        welcome.put("status", result.status());
        welcome.put("instance", instanceId.value());
        conn.send(welcome);
        // 4) resume: 시작 위치 이후 이벤트를 순서대로 보낸 뒤 실시간 전송으로 이어진다.
        conn.resumeFrom(conn.lastSentSeq());
        log.info("ws connected (headSeq={}, resumeFrom={})", result.headSeq(), hs.lastSeq());
    }

    @Override
    protected void handleTextMessage(WebSocketSession raw, TextMessage message) {
        ClientConnection conn = (ClientConnection) raw.getAttributes().get(ATTR_CONN);
        if (conn == null || conn.replaced()) {
            return;
        }
        withMdc(conn, () -> {
            Map<String, Object> frame;
            try {
                frame = json.readValue(message.getPayload(), MAP_TYPE);
            } catch (Exception e) {
                conn.send(Map.of("type", "error", "code", "INVALID_REQUEST", "message", "JSON 형식이 아닙니다."));
                return;
            }
            String type = String.valueOf(frame.get("type"));
            switch (type) {
                case "send" -> handleSend(conn, frame);
                case "typing" -> broadcaster.publish(BroadcastEnvelope.typing(conn.sessionId(), conn.userId(),
                        Boolean.TRUE.equals(frame.get("isTyping")), conn.connectionId(), instanceId.value()));
                case "resume" -> conn.resumeFrom(toLong(frame.get("lastSeq"), 0L));
                case "ping" -> conn.send(Map.of("type", "pong",
                        "headSeq", sessions.findById(conn.sessionId()).map(s -> s.lastSeq()).orElse(0L),
                        "lastSentSeq", conn.lastSentSeq()));
                default -> conn.send(Map.of("type", "error", "code", "INVALID_REQUEST",
                        "message", "알 수 없는 frame type: " + type));
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void handleSend(ClientConnection conn, Map<String, Object> frame) {
        String clientEventId = frame.get("clientEventId") == null ? null : frame.get("clientEventId").toString();
        try {
            EventType type = EventType.valueOf(String.valueOf(frame.getOrDefault("eventType", "MESSAGE")));
            if (!type.clientSubmittable() || type == EventType.JOIN) {
                throw new IllegalArgumentException(type + " 는 send 로 보낼 수 없습니다.");
            }
            Map<String, Object> payload = frame.get("payload") instanceof Map<?, ?> m
                    ? (Map<String, Object>) m : Map.of();
            Instant clientTs = frame.get("clientTs") == null ? null : Instant.parse(frame.get("clientTs").toString());
            AppendResult r = eventService.append(new AppendCommand(conn.sessionId(), conn.userId(), type,
                    clientEventId, payload, clientTs, conn.connectionId(), instanceId.value(), "ws"));
            Map<String, Object> ack = new LinkedHashMap<>();
            ack.put("type", "ack");
            ack.put("clientEventId", clientEventId);
            ack.put("outcome", r.outcome());
            ack.put("seq", r.event() == null ? null : r.event().seq());
            ack.put("serverTs", r.event() == null ? null : r.event().serverTs());
            conn.send(ack);
        } catch (ApiException e) {
            conn.send(errorFrame(clientEventId, e.code().name(), e.getMessage(), e.code().retryable()));
        } catch (IllegalArgumentException | java.time.format.DateTimeParseException e) {
            conn.send(errorFrame(clientEventId, "INVALID_REQUEST", e.getMessage(), false));
        } catch (Exception e) {
            log.error("ws send failed", e);
            conn.send(errorFrame(clientEventId, "SERVICE_UNAVAILABLE", "일시적인 오류입니다. 재시도하세요.", true));
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession raw, CloseStatus status) {
        ClientConnection conn = (ClientConnection) raw.getAttributes().get(ATTR_CONN);
        if (conn == null) {
            return;
        }
        withMdc(conn, () -> {
            registry.remove(conn);
            if (conn.replaced()) {
                return;
            }
            try {
                // 결정적 멱등 키: 이 핸들러와 장애 정리(reaper)가 둘 다 기록하려 해도 한 번만 남는다.
                eventService.append(new AppendCommand(conn.sessionId(), conn.userId(), EventType.DISCONNECT,
                        "disconnect:" + conn.connectionId(), Map.of("reason", "CLOSE_" + status.getCode()), null,
                        conn.connectionId(), instanceId.value(), "system"));
            } catch (Exception e) {
                // DB 장애 등으로 기록 실패: reaper 가 하트비트 기준으로 나중에 정리하지는 못하므로(이 인스턴스는 살아 있음)
                // 다음 연결 시 RECONNECT/takeover 로 상태가 바로잡힌다.
                log.warn("failed to record disconnect: {}", e.getMessage());
            }
            log.info("ws closed ({})", status.getCode());
        });
    }

    private static Map<String, Object> errorFrame(String clientEventId, String code, String message,
                                                  boolean retryable) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", "error");
        m.put("clientEventId", clientEventId);
        m.put("code", code);
        m.put("message", message);
        m.put("retryable", retryable);
        return m;
    }

    private void sendRaw(WebSocketSession ws, Object frame) throws Exception {
        ws.sendMessage(new TextMessage(json.writeValueAsString(frame)));
    }

    private static void withMdc(ClientConnection conn, Runnable r) {
        MDC.put("sessionId", conn.sessionId().toString());
        MDC.put("userId", conn.userId());
        MDC.put("requestId", "ws-" + conn.connectionId());
        try {
            r.run();
        } finally {
            MDC.clear();
        }
    }

    private static long toLong(Object v, long fallback) {
        if (v instanceof Number n) {
            return n.longValue();
        }
        try {
            return v == null ? fallback : Long.parseLong(v.toString());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    record Handshake(UUID sessionId, String userId, Long lastSeq) {

        static Handshake parse(URI uri) {
            if (uri == null) {
                throw new IllegalArgumentException("URI 가 없습니다.");
            }
            String path = uri.getPath();
            String idPart = path.substring(path.lastIndexOf('/') + 1);
            UUID sessionId;
            try {
                sessionId = UUID.fromString(idPart);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("세션 ID 형식이 잘못되었습니다: " + idPart);
            }
            var params = UriComponentsBuilder.fromUri(uri).build().getQueryParams();
            String userId = params.getFirst("userId");
            if (!StringUtils.hasText(userId) || userId.length() > 64) {
                throw new IllegalArgumentException("userId 쿼리 파라미터(1~64자)가 필요합니다.");
            }
            String lastSeq = params.getFirst("lastSeq");
            Long seq = null;
            if (StringUtils.hasText(lastSeq)) {
                try {
                    seq = Math.max(0, Long.parseLong(lastSeq));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("lastSeq 는 숫자여야 합니다.");
                }
            }
            return new Handshake(sessionId, userId, seq);
        }
    }
}
