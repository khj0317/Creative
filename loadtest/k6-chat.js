// k6 부하 테스트: 1:1 세션 다수에서 WebSocket 메시지 송수신 + 상태 복원 조회
//
//   docker run --rm -i --add-host=host.docker.internal:host-gateway -e BASE=http://host.docker.internal:8080 \
//     -v "$PWD/loadtest:/scripts" grafana/k6 run /scripts/k6-chat.js
//
// 각 VU = 세션 1개의 한 쪽 사용자. VU 2개가 한 세션을 공유한다(짝수/홀수).
import http from 'k6/http';
import ws from 'k6/ws';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import exec from 'k6/execution';

const BASE = __ENV.BASE || 'http://localhost:8080';
const WS_BASE = BASE.replace(/^http/, 'ws');
const SESSIONS = Number(__ENV.SESSIONS || 50);
const MESSAGES = Number(__ENV.MESSAGES || 20);

export const options = {
  scenarios: {
    chat: { executor: 'per-vu-iterations', vus: SESSIONS * 2, iterations: 1, maxDuration: '3m', exec: 'chat' },
    restore: { executor: 'constant-vus', vus: 5, duration: '40s', startTime: '5s', exec: 'restore' },
  },
  thresholds: {
    ack_latency: ['p(95)<200'],
    delivery_latency: ['p(95)<300'],
    'http_req_duration{name:timeline}': ['p(95)<300'],
    checks: ['rate>0.99'],
  },
};

const ackLatency = new Trend('ack_latency', true);
const deliveryLatency = new Trend('delivery_latency', true);
const delivered = new Counter('messages_delivered');

export function setup() {
  const ids = [];
  for (let i = 0; i < SESSIONS; i++) {
    const res = http.post(`${BASE}/sessions`, JSON.stringify({ title: `load-${i}`, invitees: [`u${i}a`, `u${i}b`] }),
      { headers: { 'content-type': 'application/json', 'x-user-id': `u${i}a` } });
    ids.push(res.json('id'));
  }
  return { ids };
}

export function chat(data) {
  const vu = exec.scenario.iterationInTest; // 시나리오 내 0..N-1 (vu.idInTest 는 다른 시나리오와 섞인다)
  const idx = Math.floor(vu / 2);
  const user = `u${idx}${vu % 2 === 0 ? 'a' : 'b'}`;
  const sid = data.ids[idx];
  const sentAt = {};

  ws.connect(`${WS_BASE}/ws/sessions/${sid}?userId=${user}&lastSeq=1`, {}, (socket) => {
    let sent = 0;
    let closing = false;
    socket.on('open', () => {
      socket.setInterval(() => {
        if (sent >= MESSAGES) { if (!closing) { closing = true; socket.setTimeout(() => socket.close(), 3000); } return; }
        const id = `${user}-${sent++}`;
        sentAt[id] = Date.now();
        socket.send(JSON.stringify({ type: 'send', clientEventId: id, eventType: 'MESSAGE',
          payload: { content: `${Date.now()}` } }));
      }, 200);
    });
    socket.on('message', (raw) => {
      const f = JSON.parse(raw);
      if (f.type === 'ack' && sentAt[f.clientEventId]) {
        ackLatency.add(Date.now() - sentAt[f.clientEventId]);
        check(f, { 'ack appended': (x) => x.outcome === 'APPENDED' || x.outcome === 'DUPLICATE' });
      } else if (f.type === 'event' && f.event.type === 'MESSAGE' && f.event.actorId !== user) {
        deliveryLatency.add(Date.now() - Number(f.event.payload.content));
        delivered.add(1);
      }
    });
  });
}

export function restore(data) {
  const sid = data.ids[Math.floor(Math.random() * data.ids.length)];
  const res = http.get(`${BASE}/sessions/${sid}/timeline?messageLimit=50`, { tags: { name: 'timeline' } });
  check(res, { 'timeline 200': (r) => r.status === 200 });
  sleep(0.1);
}
