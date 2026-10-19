#!/usr/bin/env node
// 대화형 WebSocket 테스트 클라이언트 (Node 22+, 의존성 없음)
//
//   node scripts/chat-cli.mjs --session <id> --user alice [--base http://localhost:8080]
//
// 클라이언트 측 신뢰성 동작(설계 문서의 "재연결/중복 처리" 를 클라이언트 쪽에서 구현한 예):
//   - 모든 전송에 clientEventId 를 붙이고, 3초 안에 ack 가 없으면 같은 키로 재전송(서버가 중복 제거)
//   - 연결이 끊기면 지수 백오프로 재연결하며 lastSeq 를 넘겨 놓친 이벤트를 resume
//   - 15초마다 ping → pong.headSeq 가 lastSeq 보다 크면 resume 요청(브로드캐스트 유실 대비)
//
// 입력: 텍스트 = 메시지 전송 / :edit <seq> <text> / :del <seq> / :read / :typing / :leave / :quit

import { randomUUID } from 'node:crypto';
import readline from 'node:readline';

const args = Object.fromEntries(process.argv.slice(2).reduce((acc, v, i, arr) => {
  if (v.startsWith('--')) acc.push([v.slice(2), arr[i + 1]]);
  return acc;
}, []));
if (!args.session || !args.user) {
  console.error('usage: node scripts/chat-cli.mjs --session <id> --user <name> [--base http://localhost:8080]');
  process.exit(1);
}
const base = (args.base ?? 'http://localhost:8080').replace(/^http/, 'ws');
let lastSeq = args.lastSeq ? Number(args.lastSeq) : null;
let ws;
let attempt = 0;
let quitting = false;
const pending = new Map(); // clientEventId → { frame, timer, tries }

const log = (...a) => { process.stdout.write('\r\x1b[K'); console.log(...a); rl?.prompt(true); };

function render(e) {
  const t = new Date(e.serverTs).toLocaleTimeString();
  const p = e.payload ?? {};
  switch (e.type) {
    case 'MESSAGE': return `[${e.seq}] ${t} ${e.actorId}: ${p.content}`;
    case 'MESSAGE_EDITED': return `[${e.seq}] ${t} ${e.actorId} 수정 #${p.targetSeq}: ${p.content}`;
    case 'MESSAGE_DELETED': return `[${e.seq}] ${t} ${e.actorId} 삭제 #${p.targetSeq}`;
    case 'READ': return `[${e.seq}] ${t} ${e.actorId} 읽음 ~#${p.upToSeq}`;
    default: return `[${e.seq}] ${t} ── ${e.actorId} ${e.type} ${JSON.stringify(p)}`;
  }
}

function connect() {
  const url = `${base}/ws/sessions/${args.session}?userId=${encodeURIComponent(args.user)}`
    + (lastSeq != null ? `&lastSeq=${lastSeq}` : '');
  ws = new WebSocket(url);
  ws.onopen = () => { attempt = 0; };
  ws.onmessage = (m) => {
    const f = JSON.parse(m.data);
    switch (f.type) {
      case 'welcome':
        log(`** 연결됨 instance=${f.instance} status=${f.status} headSeq=${f.headSeq}`);
        for (const p of pending.values()) ws.send(JSON.stringify(p.frame)); // 미확인 전송 재시도
        break;
      case 'event':
        lastSeq = f.event.seq;
        log(render(f.event));
        break;
      case 'ack': {
        const p = pending.get(f.clientEventId);
        if (p) { clearTimeout(p.timer); pending.delete(f.clientEventId); }
        if (f.outcome === 'DUPLICATE') log(`   (재전송 중복 처리됨 seq=${f.seq})`);
        break;
      }
      case 'error': {
        const p = pending.get(f.clientEventId);
        if (p && !f.retryable) { clearTimeout(p.timer); pending.delete(f.clientEventId); }
        log(`!! ${f.code}: ${f.message}${f.retryable ? ' (재시도 예정)' : ''}`);
        break;
      }
      case 'typing': log(`   ${f.userId} ${f.isTyping ? '입력 중…' : ''}`); break;
      case 'pong':
        if (lastSeq != null && f.headSeq > lastSeq) ws.send(JSON.stringify({ type: 'resume', lastSeq }));
        break;
    }
  };
  ws.onclose = (ev) => {
    if (quitting) return;
    const delay = Math.min(30000, 500 * 2 ** attempt++) * (0.5 + Math.random() / 2);
    log(`** 연결 끊김(${ev.code}) → ${Math.round(delay)}ms 후 재연결 (lastSeq=${lastSeq})`);
    setTimeout(connect, delay);
  };
}

function send(eventType, payload) {
  const clientEventId = randomUUID();
  const frame = { type: 'send', clientEventId, eventType, payload, clientTs: new Date().toISOString() };
  const entry = { frame, tries: 0 };
  const retry = () => {
    if (!pending.has(clientEventId)) return;
    if (++entry.tries > 5) { pending.delete(clientEventId); log(`!! 전송 포기 ${clientEventId}`); return; }
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(frame));
    entry.timer = setTimeout(retry, 3000);
  };
  pending.set(clientEventId, entry);
  retry();
}

const rl = readline.createInterface({ input: process.stdin, output: process.stdout, prompt: `${args.user}> ` });
rl.on('line', (line) => {
  const s = line.trim();
  if (!s) return rl.prompt();
  const [cmd, a1, ...rest] = s.split(' ');
  switch (cmd) {
    case ':edit': send('MESSAGE_EDITED', { targetSeq: Number(a1), content: rest.join(' ') }); break;
    case ':del': send('MESSAGE_DELETED', { targetSeq: Number(a1) }); break;
    case ':read': send('READ', { upToSeq: lastSeq ?? 0 }); break;
    case ':leave': send('LEAVE', {}); break;
    case ':typing': ws.send(JSON.stringify({ type: 'typing', isTyping: true })); break;
    case ':quit': quitting = true; ws.close(1000); rl.close(); process.exit(0); break;
    default: send('MESSAGE', { content: s });
  }
  rl.prompt();
});
setInterval(() => ws?.readyState === WebSocket.OPEN && ws.send(JSON.stringify({ type: 'ping' })), 15000);
connect();
rl.prompt();
