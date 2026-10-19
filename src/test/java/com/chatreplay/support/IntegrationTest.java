package com.chatreplay.support;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** 실제 서버(랜덤 포트) + Testcontainers PostgreSQL 위에서 HTTP/WebSocket 으로 검증한다. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
public abstract class IntegrationTest {

    @Value("${local.server.port}")
    protected int port;

    @Autowired
    protected JsonMapper json;

    protected HttpClient http;

    @BeforeEach
    void setUpClient() {
        http = HttpClient.newHttpClient();
    }

    public record Res(int status, JsonNode body) {
    }

    protected Res get(String path) {
        return send(HttpRequest.newBuilder(uri(path)).GET().build());
    }

    protected Res post(String path, String user, Object body) {
        HttpRequest.Builder b = HttpRequest.newBuilder(uri(path))
                .header("Content-Type", "application/json")
                .POST(body == null ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(json.writeValueAsString(body)));
        if (user != null) {
            b.header("X-User-Id", user);
        }
        return send(b.build());
    }

    private Res send(HttpRequest req) {
        try {
            HttpResponse<String> r = http.send(req, HttpResponse.BodyHandlers.ofString());
            JsonNode node = r.body() == null || r.body().isBlank() ? null : json.readTree(r.body());
            return new Res(r.statusCode(), node);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    protected URI uri(String path) {
        return URI.create("http://localhost:" + port + path);
    }

    protected URI wsUri(UUID sessionId, String userId, Long lastSeq) {
        return URI.create("ws://localhost:" + port + "/ws/sessions/" + sessionId + "?userId=" + userId
                + (lastSeq == null ? "" : "&lastSeq=" + lastSeq));
    }

    // ---- 도메인 도우미 ----

    protected UUID createSession(String creator, List<String> invitees) {
        Res r = post("/sessions", creator, Map.of("title", "test", "invitees", invitees));
        if (r.status() != 201) {
            throw new IllegalStateException("create failed: " + r);
        }
        return UUID.fromString(r.body().get("id").asString());
    }

    protected void join(UUID sid, String user) {
        Res r = post("/sessions/" + sid + "/join", user, null);
        if (r.status() != 200) {
            throw new IllegalStateException("join failed: " + r);
        }
    }

    /** 단건 이벤트를 수집 API 로 보내고 항목 결과를 돌려준다. */
    protected JsonNode sendEvent(UUID sid, String user, String clientEventId, String type, Map<String, Object> payload) {
        Res r = post("/sessions/" + sid + "/events", user, Map.of("events", List.of(
                Map.of("clientEventId", clientEventId, "type", type, "payload", payload))));
        if (r.status() != 200) {
            throw new IllegalStateException("ingest failed: " + r);
        }
        return r.body().get("results").get(0);
    }

    protected long sendMessage(UUID sid, String user, String clientEventId, String content) {
        JsonNode item = sendEvent(sid, user, clientEventId, "MESSAGE", Map.of("content", content));
        return item.get("seq").asLong();
    }

    protected <T> T convert(JsonNode node, TypeReference<T> type) {
        return json.convertValue(node, type);
    }
}
