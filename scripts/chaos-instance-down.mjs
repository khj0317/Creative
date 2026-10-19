#!/usr/bin/env node
// 장애 주입: 인스턴스 강제 종료 시 presence 정리(reaper) 검증. cluster 프로필이 떠 있어야 한다.
//
//   node scripts/chaos-instance-down.mjs
//
// 1) alice → app-1, bob → app-2 로 연결
// 2) docker kill app-2  (afterConnectionClosed 가 호출되지 않는 비정상 종료)
// 3) app-1 의 reaper 가 하트비트 만료(20s)를 감지해 bob 에 DISCONNECT(reason=INSTANCE_DOWN) 기록 → INTERRUPTED
// 4) bob 이 LB(살아 있는 인스턴스)로 lastSeq 재연결 → RECONNECT, ACTIVE
// 5) app-2 재기동
import { execSync } from 'node:child_process';

const A = 'http://localhost:8081';
const LB = 'http://localhost:8088';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const api = async (base, method, path, user, body) => {
  const r = await fetch(base + path, { method, headers: { 'content-type': 'application/json', ...(user ? { 'x-user-id': user } : {}) }, body: body && JSON.stringify(body) });
  return r.json();
};
const open = (base, sid, user, lastSeq) => new Promise((resolve) => {
  const ws = new WebSocket(`${base.replace('http', 'ws')}/ws/sessions/${sid}?userId=${user}${lastSeq != null ? `&lastSeq=${lastSeq}` : ''}`);
  ws.events = [];
  ws.onmessage = (m) => { const f = JSON.parse(m.data); if (f.type === 'event') ws.events.push(f.event); if (f.type === 'welcome') resolve(Object.assign(ws, { instance: f.instance })); };
  ws.onerror = () => {};
});
const bobState = async (sid) => (await api(A, 'GET', `/sessions/${sid}`)).participants.find((p) => p.userId === 'bob');

const sid = (await api(A, 'POST', '/sessions', 'alice', { invitees: ['alice', 'bob'] })).id;
const alice = await open(A, sid, 'alice');
const bob = await open('http://localhost:8082', sid, 'bob');
console.log(`alice@${alice.instance}, bob@${bob.instance}, session ${sid}`);
await sleep(300);
const lastSeq = Math.max(...bob.events.map((e) => e.seq), 0);

console.log('docker kill app-2 ...');
execSync('docker compose kill app-2', { stdio: 'inherit' });
const t0 = Date.now();
let state;
for (;;) {
  state = await bobState(sid);
  if (state.presence !== 'ONLINE' || Date.now() - t0 > 60000) break;
  await sleep(1000);
}
const disconnect = alice.events.find((e) => e.type === 'DISCONNECT' && e.actorId === 'bob');
console.log(`${((Date.now() - t0) / 1000).toFixed(1)}s 후 bob presence=${state.presence}, reason=${disconnect?.payload.reason}`);
console.log(`세션 상태: ${(await api(A, 'GET', `/sessions/${sid}`)).status}`);

const bob2 = await open(LB, sid, 'bob', lastSeq);
await sleep(500);
console.log(`bob 재연결@${bob2.instance} → presence=${(await bobState(sid)).presence}, 세션 상태=${(await api(A, 'GET', `/sessions/${sid}`)).status}`);
alice.close(); bob2.close();

execSync('docker compose start app-2', { stdio: 'inherit' });
const ok = state.presence === 'DISCONNECTED' && disconnect?.payload.reason === 'INSTANCE_DOWN';
console.log(ok ? '✅ 인스턴스 장애 시 presence 정리 확인' : '❌ 검증 실패');
process.exit(ok ? 0 : 1);
