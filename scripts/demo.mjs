#!/usr/bin/env node
// 시나리오 재현 스크립트 (Node 22+ 내장 fetch/WebSocket, 의존성 없음)
//
//   node scripts/demo.mjs                                  # 단일 인스턴스(localhost:8080)
//   node scripts/demo.mjs --a http://localhost:8081 --b http://localhost:8082   # alice/bob 을 서로 다른 인스턴스에 연결
//
// 검증 항목: 실시간 송수신, ack/중복 재전송, 순서 뒤바뀜(수정이 원본보다 먼저), 연결 끊김 → INTERRUPTED,
//            lastSeq 재연결 resume, 특정 시점 복원(AUTO vs FULL 동일성), 세션 종료 멱등성.

import { randomUUID } from 'node:crypto';

const args = Object.fromEntries(process.argv.slice(2).reduce((acc, v, i, arr) => {
  if (v.startsWith('--')) acc.push([v.slice(2), arr[i + 1]]);
  return acc;
}, []));
const A = args.a ?? args.base ?? 'http://localhost:8080';
const B = args.b ?? A;

let failures = 0;
const check = (cond, label) => {
  console.log(`${cond ? '  PASS' : '  FAIL'}  ${label}`);
  if (!cond) failures++;
};
const step = (title) => console.log(`\n▶ ${title}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function api(base, method, path, user, body) {
  const res = await fetch(base + path, {
    method,
    headers: { 'content-type': 'application/json', ...(user ? { 'x-user-id': user } : {}) },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : null };
}

class Client {
  constructor(base, sessionId, user) {
    this.base = base; this.sessionId = sessionId; this.user = user;
    this.frames = []; this.waiters = []; this.lastSeq = null;
  }
  connect(lastSeq) {
    const url = `${this.base.replace(/^http/, 'ws')}/ws/sessions/${this.sessionId}?userId=${this.user}`
      + (lastSeq != null ? `&lastSeq=${lastSeq}` : '');
    this.ws = new WebSocket(url);
    this.ws.onmessage = (m) => {
      const f = JSON.parse(m.data);
      if (f.type === 'event') this.lastSeq = f.event.seq;
      this.frames.push(f);
      this.waiters = this.waiters.filter((w) => !w(f));
    };
    return this.waitFor((f) => f.type === 'welcome');
  }
  waitFor(pred, timeoutMs = 5000) {
    const found = this.frames.find(pred);
    if (found) { this.frames.splice(this.frames.indexOf(found), 1); return Promise.resolve(found); }
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error(`${this.user}: timeout waiting frame`)), timeoutMs);
      this.waiters.push((f) => {
        if (!pred(f)) return false;
        clearTimeout(t); this.frames.splice(this.frames.indexOf(f), 1); resolve(f); return true;
      });
    });
  }
  async send(eventType, payload, clientEventId = randomUUID()) {
    this.ws.send(JSON.stringify({ type: 'send', clientEventId, eventType, payload, clientTs: new Date().toISOString() }));
    return this.waitFor((f) => (f.type === 'ack' || f.type === 'error') && f.clientEventId === clientEventId);
  }
  event(type) { return this.waitFor((f) => f.type === 'event' && f.event.type === type).then((f) => f.event); }
  close() { this.ws.close(1000, 'bye'); }
}

step(`세션 생성 (alice → ${A}, bob → ${B})`);
const created = await api(A, 'POST', '/sessions', 'alice', { title: 'demo', invitees: ['alice', 'bob'] });
check(created.status === 201, `POST /sessions → 201 (id=${created.body?.id})`);
const sid = created.body.id;

step('WebSocket 연결 + 참여');
const alice = new Client(A, sid, 'alice');
const bob = new Client(B, sid, 'bob');
const wa = await alice.connect();
const wb = await bob.connect();
console.log(`  alice@${wa.instance}, bob@${wb.instance}`);
await alice.event('JOIN'); // bob 참여 알림(다른 인스턴스라면 Redis Pub/Sub 경유)
check(true, 'alice 가 bob 의 JOIN 을 실시간 수신');

step('메시지 송수신 + 중복 재전송');
const key = randomUUID();
const ack1 = await alice.send('MESSAGE', { content: '안녕하세요 bob' }, key);
const got = await bob.event('MESSAGE');
check(ack1.outcome === 'APPENDED' && got.seq === ack1.seq, `bob 수신 seq=${got.seq}, content="${got.payload.content}"`);
const ack2 = await alice.send('MESSAGE', { content: '안녕하세요 bob' }, key);
check(ack2.outcome === 'DUPLICATE' && ack2.seq === ack1.seq, `같은 clientEventId 재전송 → DUPLICATE (seq=${ack2.seq})`);

step('순서 뒤바뀜: 수정 이벤트가 원본보다 먼저 도착');
const editKey = randomUUID();
const early = await bob.send('MESSAGE_EDITED', { targetSeq: ack1.seq + 999, content: 'x' }, editKey);
check(early.type === 'error' && early.code === 'TARGET_NOT_FOUND' && early.retryable, `거절(${early.code}, retryable=${early.retryable})`);
const bobMsg = await bob.send('MESSAGE', { content: '반가워요 alice' });
const edited = await bob.send('MESSAGE_EDITED', { targetSeq: bobMsg.seq, content: '반가워요 alice!!' }, editKey);
check(edited.outcome === 'APPENDED', `원본 저장 후 같은 키로 재시도 → 반영 (seq=${edited.seq})`);
await alice.event('MESSAGE_EDITED');

const beforeDisconnect = (await api(A, 'GET', `/sessions/${sid}`)).body;
const markerAt = beforeDisconnect.lastEventAt;

step('bob 연결 끊김 → INTERRUPTED');
const lastSeen = bob.lastSeq;
bob.close();
await alice.event('DISCONNECT');
const interrupted = (await api(A, 'GET', `/sessions/${sid}`)).body;
check(interrupted.status === 'INTERRUPTED', `세션 상태 = ${interrupted.status}`);

step('끊긴 동안 alice 가 보낸 메시지');
const missed = [];
for (let i = 1; i <= 3; i++) missed.push((await alice.send('MESSAGE', { content: `놓친 메시지 ${i}` })).seq);

step(`bob 재연결 (lastSeq=${lastSeen}) → resume`);
const bob2 = new Client(B, sid, 'bob');
await bob2.connect(lastSeen);
const resumed = [];
let prev = lastSeen; let ordered = true;
while (resumed.length < missed.length) {
  const e = (await bob2.waitFor((f) => f.type === 'event')).event;
  ordered &&= e.seq === prev + 1; prev = e.seq;
  if (e.type === 'MESSAGE') resumed.push(e.seq);
}
check(JSON.stringify(resumed) === JSON.stringify(missed), `놓친 메시지 ${resumed.join(',')} 를 빈틈없이 순서대로 수신`);
check(ordered, 'resume 구간 seq 연속');
await alice.event('RECONNECT');
check((await api(A, 'GET', `/sessions/${sid}`)).body.status === 'ACTIVE', '재연결 후 ACTIVE');

step('삭제/읽음/종료');
await alice.send('MESSAGE_DELETED', { targetSeq: missed[0] });
const head = (await api(A, 'GET', `/sessions/${sid}`)).body.headSeq;
await bob2.send('READ', { upToSeq: head });
const end1 = await api(A, 'POST', `/sessions/${sid}/end`, 'alice');
const end2 = await api(A, 'POST', `/sessions/${sid}/end`, 'alice');
check(end1.body.outcome === 'APPENDED' && end2.body.outcome === 'DUPLICATE', `종료 멱등 (${end1.body.outcome} → ${end2.body.outcome})`);

step('특정 시점 상태 복원');
const past = (await api(A, 'GET', `/sessions/${sid}/timeline?at=${encodeURIComponent(markerAt)}`)).body;
check(past.status === 'ACTIVE' && past.totalMessages === 2, `at=${markerAt} → status=${past.status}, messages=${past.totalMessages}`);
check(past.messages[1].content === '반가워요 alice!!', '그 시점에는 수정이 반영된 상태');
const now = (await api(A, 'GET', `/sessions/${sid}/timeline`)).body;
check(now.status === 'COMPLETED', `현재 status=${now.status}, messages=${now.totalMessages}`);
check(now.messages.find((m) => m.seq === missed[0]).status === 'DELETED', '삭제 반영');
const bobView = now.participants.find((p) => p.userId === 'bob');
check(bobView.unreadCount === 0, `bob unread=${bobView.unreadCount}`);

let same = true;
for (let s = 1; s <= now.seq; s++) {
  const auto = (await api(A, 'GET', `/sessions/${sid}/timeline?atSeq=${s}&messageLimit=500`)).body;
  const full = (await api(A, 'GET', `/sessions/${sid}/timeline?atSeq=${s}&messageLimit=500&mode=FULL`)).body;
  delete auto.restore; delete full.restore;
  same &&= JSON.stringify(auto) === JSON.stringify(full);
}
check(same, `seq 1..${now.seq} 모든 시점에서 스냅샷 복원 == 전체 리플레이`);

alice.close(); bob2.close();
await sleep(200);
console.log(`\n${failures === 0 ? '✅ 모든 검증 통과' : `❌ 실패 ${failures}건`}  (session ${sid})`);
process.exit(failures === 0 ? 0 : 1);
