package com.chatreplay.realtime;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.chatreplay.event.EventRepository;
import com.chatreplay.event.SessionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import tools.jackson.databind.json.JsonMapper;

/**
 * WebSocket 연결 1개. 클라이언트에게 이벤트를 "seq 순서대로, 빈틈 없이, 한 번씩" 보내는 책임을 진다.
 *
 * <ul>
 *   <li>메일박스(actor) 방식: 브로드캐스트 수신 스레드는 inbox 에 넣기만 하고, 공용 전달 풀이 연결별로 한 번에 하나씩 비운다.
 *       느린 클라이언트가 수신 스레드나 다른 연결을 막지 않는다.</li>
 *   <li>lastSentSeq 이하 이벤트는 버린다(중복 브로드캐스트, resume 과 실시간 전송의 겹침).</li>
 *   <li>재정렬 버퍼: lastSentSeq+1 보다 큰 이벤트는 잠시(REORDER_WAIT) 보관한다. 인스턴스 간 publish 순서가
 *       뒤바뀐 경우 빠진 이벤트가 곧 도착하므로 DB 를 치지 않고 정렬만으로 해결된다.
 *       대기 후에도 빈틈이면 DB 에서 메운다(Pub/Sub 유실). seq N 이 커밋됐다면 N 미만도 모두 커밋돼 있다.</li>
 * </ul>
 */
public class ClientConnection {

    private static final Logger log = LoggerFactory.getLogger(ClientConnection.class);
    private static final int PAGE = 500;
    private static final long REORDER_WAIT_MS = 100;

    private final WebSocketSession ws;
    private final UUID sessionId;
    private final String userId;
    private final String connectionId;
    private final EventRepository events;
    private final JsonMapper json;
    private final ScheduledExecutorService executor;
    private final ReentrantLock lock = new ReentrantLock();
    private final Queue<SessionEvent> inbox = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean scheduled = new AtomicBoolean();
    private final AtomicLong syncTarget = new AtomicLong();
    /** 순서가 앞선 이벤트를 기다리는 중인 이벤트들(lock 보호) */
    private final TreeMap<Long, SessionEvent> pending = new TreeMap<>();
    private long gapSinceNanos;
    private volatile long lastSentSeq;
    private volatile boolean replaced;

    public ClientConnection(WebSocketSession ws, UUID sessionId, String userId, String connectionId,
                            long startSeq, EventRepository events, JsonMapper json,
                            ScheduledExecutorService executor) {
        this.ws = ws;
        this.sessionId = sessionId;
        this.userId = userId;
        this.connectionId = connectionId;
        this.lastSentSeq = startSeq;
        this.events = events;
        this.json = json;
        this.executor = executor;
    }

    /** 재연결 resume: fromSeq 이후의 이벤트를 모두 보낸 뒤 실시간 전송으로 이어진다. */
    public void resumeFrom(long fromSeq) {
        lock.lock();
        try {
            // fromSeq 가 더 작으면 클라이언트가 앞에서부터 다시 받고 싶다는 뜻(로컬 저장소 유실 등)
            lastSentSeq = fromSeq;
            pending.clear();
            catchUp(Long.MAX_VALUE);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 연결 수립(welcome + resume)이 끝날 때까지 실시간 전달을 막는다.
     * 그 사이 도착한 이벤트는 inbox 에 쌓였다가 releaseDelivery 후 seq 규칙에 따라 처리된다.
     */
    public void holdDelivery() {
        lock.lock();
    }

    public void releaseDelivery() {
        lock.unlock();
        schedule();
    }

    /** 브로드캐스트 수신 측에서 호출. 메일박스에 넣고 즉시 반환한다. */
    public void enqueue(SessionEvent e) {
        inbox.add(e);
        schedule();
    }

    /**
     * 안전망: DB 의 head 가 마지막 전송 seq 보다 앞서 있으면 따라잡는다.
     * Pub/Sub 메시지가 유실되고 그 뒤로 이벤트가 더 오지 않는 경우(대화의 마지막 메시지)를 메운다.
     */
    public void syncTo(long headSeq) {
        syncTarget.accumulateAndGet(headSeq, Math::max);
        schedule();
    }

    private void schedule() {
        if (scheduled.compareAndSet(false, true)) {
            executor.execute(this::drain);
        }
    }

    private void drain() {
        if (!lock.tryLock()) {
            // 연결 수립 중(holdDelivery): releaseDelivery 가 다시 schedule 한다.
            scheduled.set(false);
            if (!lock.isLocked()) {
                schedule();
            }
            return;
        }
        boolean waitForGap = false;
        try {
            SessionEvent e;
            while ((e = inbox.poll()) != null) {
                if (e.seq() > lastSentSeq) {
                    pending.put(e.seq(), e);
                }
            }
            flushContiguous();
            if (!pending.isEmpty()) {
                long now = System.nanoTime();
                if (gapSinceNanos == 0) {
                    gapSinceNanos = now;
                }
                if (now - gapSinceNanos >= TimeUnit.MILLISECONDS.toNanos(REORDER_WAIT_MS)) {
                    catchUp(pending.firstKey() - 1); // 기다려도 안 온 이벤트는 DB 에서
                    flushContiguous();
                } else {
                    waitForGap = true;
                }
            }
            long target = syncTarget.get();
            if (target > lastSentSeq) {
                catchUp(target);
                flushContiguous();
            }
            if (pending.isEmpty()) {
                gapSinceNanos = 0;
            }
        } catch (RuntimeException ex) {
            log.warn("delivery failed on connection {}: {}", connectionId, ex.getMessage());
        } finally {
            lock.unlock();
            scheduled.set(false);
        }
        if (!inbox.isEmpty()) {
            schedule();
        } else if (waitForGap) {
            executor.schedule(this::schedule, REORDER_WAIT_MS, TimeUnit.MILLISECONDS);
        }
    }

    private void flushContiguous() {
        pending.headMap(lastSentSeq, true).clear();
        SessionEvent next;
        while ((next = pending.remove(lastSentSeq + 1)) != null) {
            sendEvent(next);
        }
    }

    private void catchUp(long toSeq) {
        while (lastSentSeq < toSeq && ws.isOpen()) {
            List<SessionEvent> page = events.findRange(sessionId, lastSentSeq, toSeq, PAGE);
            if (page.isEmpty()) {
                break;
            }
            for (SessionEvent e : page) {
                sendEvent(e);
            }
        }
        pending.headMap(lastSentSeq, true).clear();
    }

    private void sendEvent(SessionEvent e) {
        send(Map.of("type", "event", "event", e));
        lastSentSeq = e.seq();
    }

    public void send(Object frame) {
        if (!ws.isOpen()) {
            return;
        }
        try {
            ws.sendMessage(new TextMessage(json.writeValueAsString(frame)));
        } catch (IOException | IllegalStateException ex) {
            log.debug("send failed on connection {}: {}", connectionId, ex.getMessage());
            close(CloseStatus.SESSION_NOT_RELIABLE);
        }
    }

    public void close(CloseStatus status) {
        try {
            if (ws.isOpen()) {
                ws.close(status);
            }
        } catch (IOException ignored) {
            // 이미 닫힘
        }
    }

    public void markReplaced() {
        replaced = true;
    }

    public boolean replaced() {
        return replaced;
    }

    public UUID sessionId() {
        return sessionId;
    }

    public String userId() {
        return userId;
    }

    public String connectionId() {
        return connectionId;
    }

    public long lastSentSeq() {
        return lastSentSeq;
    }
}
