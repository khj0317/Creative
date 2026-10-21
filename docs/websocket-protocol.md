# WebSocket 프로토콜

```
ws://{host}/ws/sessions/{sessionId}?userId={userId}[&lastSeq={n}]
```
- `userId`: 사용자 식별(인증은 과제 범위 밖, 운영에서는 토큰 검증)
- `lastSeq`: 마지막으로 받은 seq. 주면 그 **이후 이벤트를 모두 순서대로 재전송한 뒤** 실시간 전송으로 이어진다(resume). 생략하면 연결 시점 이후부터 받는다. 서버 head 보다 큰 값(다른 세션의 값, 로컬 저장소 오염 등)은 head 로 제한한다. 실제로 쓰인 시작 위치는 `welcome.resumeFromSeq` 로 알려 준다.
- 연결 자체가 참여/presence 다. 참여 전이면 `JOIN`(presence ONLINE), 오프라인/단절 상태였다면 `RECONNECT` 가 기록된다.

## Client → Server

| type | 필드 | 설명 |
|---|---|---|
| `send` | `clientEventId`(필수, 멱등 키), `eventType`(MESSAGE 기본 / MESSAGE_EDITED / MESSAGE_DELETED / READ / LEAVE), `payload`, `clientTs`(선택) | 이벤트 전송. 응답으로 `ack` 또는 `error`. LEAVE 는 ack 뒤 연결이 닫힌다 |
| `typing` | `isTyping` | 입력 중 표시. 저장하지 않고 상대에게만 전달 |
| `resume` | `lastSeq` | 그 이후 이벤트 재전송 요청(뒤처짐을 감지했을 때). head 보다 크면 head 로 제한 |
| `ping` | — | `pong { headSeq, lastSentSeq }` 응답. `headSeq > 내 lastSeq` 이면 `resume` |

payload:
- MESSAGE `{content}`
- MESSAGE_EDITED `{targetSeq, content}`
- MESSAGE_DELETED `{targetSeq}`
- READ `{upToSeq}`

## Server → Client

| type | 필드 | 설명 |
|---|---|---|
| `welcome` | `connectionId, headSeq, resumeFromSeq, status, instance` | 연결 수립 완료. 그 뒤 `resumeFromSeq` 이후 이벤트가 이어진다 |
| `event` | `event { sessionId, seq, eventId, clientEventId, type, actorId, payload, clientTs, serverTs }` | **seq 오름차순, 빈틈 없이, 한 번씩** 전달된다 |
| `ack` | `clientEventId, outcome(APPENDED/DUPLICATE/SKIPPED), seq, serverTs` | 전송 결과. DUPLICATE 면 처음 저장된 seq |
| `error` | `clientEventId, code, message, retryable` | 예: `TARGET_NOT_FOUND`(원본이 아직 없음, retryable), `TARGET_NOT_A_MESSAGE`(재시도 불가), `NOT_A_PARTICIPANT`, `SESSION_ENDED` |
| `typing` | `userId, isTyping` | 상대의 입력 중 표시 |
| `pong` | `headSeq, lastSentSeq` | |

## 종료 코드
| 코드 | 의미 |
|---|---|
| 1000 / 1001 | 정상 종료(퇴장 포함: reason `left the session`) / 서버 종료(배포). 1001 이면 즉시 재연결 |
| 4001 | 같은 사용자의 새 연결로 대체됐거나 다른 연결/REST 로 퇴장함. 재연결하지 않는다 |
| 44xx | 연결 거부(예: 4403 `NOT_INVITED`, 4404 `SESSION_NOT_FOUND`, 4409 `SESSION_FULL`/`SESSION_ENDED`) |

## 클라이언트 권장 동작 (`scripts/chat-cli.mjs` 에 구현)
1. 모든 전송에 UUID `clientEventId` 를 붙이고 ack 를 기다린다. 3초 안에 ack 가 없거나 `retryable` 오류면 **같은 키로** 재전송한다.
2. 받은 `event.seq` 를 `lastSeq` 로 저장한다. 연결이 끊기면 지수 백오프 + jitter 로 재연결하면서 `lastSeq` 를 넘긴다.
3. 재연결 `welcome` 을 받으면 ack 를 못 받은 전송을 다시 보낸다(서버가 중복 제거).
4. 주기적으로 `ping` 을 보내고, `pong.headSeq > lastSeq` 면 `resume` 을 보낸다.
