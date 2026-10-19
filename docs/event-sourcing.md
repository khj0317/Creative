# 이벤트 기반 상태 복원

> 구현: `replay/SessionStateReducer`, `replay/RestoreService`, `projection/ProjectionService`(스냅샷 자동화)
> API: `GET /sessions/{id}/timeline?at=<ISO시각> | atSeq=<n> [&messageLimit=50] [&mode=AUTO|FULL]`

## 1. 복원 대상

| 상태 | 반영 이벤트 |
|---|---|
| 세션 상태 (WAITING / ACTIVE / INTERRUPTED / COMPLETED) | JOIN, LEAVE, DISCONNECT, RECONNECT, SESSION_ENDED |
| 참여자 목록 + 참여 상태(JOINED/LEFT) + presence(ONLINE/OFFLINE/DISCONNECTED) | JOIN, LEAVE, DISCONNECT, RECONNECT |
| 메시지 목록(최근 N개 + 전체 개수) | MESSAGE |
| 메시지 상태(SENT / EDITED(+editCount) / DELETED(본문 제거, 툼스톤 유지)) | MESSAGE_EDITED, MESSAGE_DELETED |
| 참여자별 읽음 위치(lastReadSeq)와 unread count | READ |

typing indicator 는 저장하지 않는 휘발성 신호라서 복원 대상이 아니다. 저장하면 이벤트 양이 몇 배로 늘어나는데, 과거 시점의 "입력 중" 표시는 쓸모가 없다.

응답 예시(요약):
```json
{
  "sessionId": "…", "seq": 13, "stateAt": "2026-10-19T08:19:36.586010Z", "status": "ACTIVE",
  "participants": [
    {"userId": "alice", "state": "JOINED", "presence": "ONLINE", "online": true, "lastReadSeq": 0, "unreadCount": 1},
    {"userId": "bob",   "state": "JOINED", "presence": "DISCONNECTED", "online": false, "lastReadSeq": 8, "unreadCount": 2}
  ],
  "totalMessages": 5,
  "messages": [{"seq": 4, "senderId": "alice", "content": "hi!", "status": "EDITED", "editCount": 1, "…": "…"}],
  "restore": {"strategy": "SNAPSHOT_REPLAY", "snapshotSeq": 100, "replayedEvents": 13, "elapsedMicros": 812}
}
```

## 2. 결정성(Determinism)을 보장하는 규칙

`state' = reduce(state, event)` 는 순수 함수다. 같은 이벤트 열(seq 순)이면 **언제, 어느 인스턴스에서, 스냅샷을 쓰든 안 쓰든** 같은 상태가 나온다.

1. **정렬 기준은 seq 하나뿐**이다. 클라이언트 시각은 쓰지 않는다.
2. **시각은 이벤트의 `server_ts` 만** 쓴다. 리듀서 안에서 `now()` 를 호출하지 않는다.
3. **정렬된 컬렉션**만 쓴다(참여자는 userId 순 `TreeMap`, 메시지는 seq 순). 해시 순서에 의존하지 않는다.
4. **payload 정규화**: 저장 시 허용 필드만 남긴다(예: MESSAGE 는 `content` 만). 해석이 입력 잡음에 흔들리지 않는다.
5. **상태 규칙 공유**: 세션 상태 계산(`SessionStatus.derive`)을 쓰기 모델과 리듀서가 같은 함수로 쓴다.
6. **스냅샷 형식 버전**(`state_version`): 형식이 바뀌면 옛 스냅샷은 무시하고 리플레이로 우회한다. 결과는 같고 느려질 뿐이다.

## 3. 전체 리플레이 vs 스냅샷 + 리플레이 → **스냅샷 + 리플레이(기본) + 전체 리플레이(검증용)**

| | 전체 리플레이 | 스냅샷 + 리플레이 (채택) |
|---|---|---|
| 비용 | O(목표 seq 까지의 이벤트 수) | O(스냅샷 역직렬화) + O(스냅샷 이후 이벤트 ≤ interval-1) |
| 정합성 위험 | 없음(이벤트만 진실) | 스냅샷 손상/버그 → **FULL 과 비교 검증**, 버전으로 무효화 가능 |
| 저장 비용 | 없음 | 스냅샷 × 상태 크기 |
| 구현 | 단순 | 스냅샷 생성(비동기) 필요 |

```
restore(session, target):
  1) target = (at 이 있으면) max(seq | server_ts <= at)        -- Index Only Scan 1행
  2) snap   = latest snapshot where seq <= target and version 일치  -- PK 역방향 1행
  3) state  = snap ? deserialize(snap) : empty
  4) for page in events(seq ∈ (state.seq, target]) ORDER BY seq LIMIT 1000:   -- PK 범위 스캔
         state = reduce(state, e)
```

- `mode=FULL` 은 같은 API 로 전체 리플레이를 강제한다. **통합 테스트와 데모 스크립트는 세션의 모든 seq 에서 `AUTO == FULL` 을 비교한다.** 스냅샷 경로의 버그나 손상을 잡는 장치다.
- 응답 `restore` 필드에 쓰인 전략, 스냅샷 seq, 리플레이한 이벤트 수, 서버 처리 시간을 담아 비용을 관측할 수 있다. Micrometer `chat_timeline_restore_seconds{strategy}` 에도 기록한다.

## 4. 중복 이벤트와 순서 뒤바뀜을 복원 로직에서 다루는 방법

쓰기 경로가 1차로 막고(멱등 키, seq 직렬화, 대상 검증), 리듀서가 2차로 방어한다.

| 상황 | 쓰기 경로 | 복원(리듀서) |
|---|---|---|
| 같은 이벤트 재전송 | 멱등 키로 저장 안 함 | 저장된 적이 없으므로 리플레이에도 없음 |
| 같은 이벤트가 리플레이 입력에 두 번 등장(스냅샷 경계 겹침, 페이지 중복) | — | `seq <= state.seq` 는 무시 → 재적용 멱등 |
| 이벤트 사이 빈틈(seq 누락) | 잠금 안 부여 + 롤백 시 재사용으로 생기지 않음 | **예외로 실패**: 조용히 틀린 상태를 만들지 않음 |
| 수정/삭제가 원본보다 먼저 도착 | `TARGET_NOT_FOUND`(재시도 가능) 거절 | 존재하지 않는/삭제된/남의 메시지에 대한 수정·삭제는 no-op(이중 방어) |
| 오래된 READ 가 늦게 도착 | 그대로 저장(seq 는 도착 순) | `lastReadSeq = max(...)` 라 되돌아가지 않음 |

단위 테스트(`SessionStateReducerTest`)가 다음을 확인한다.
- 이벤트를 2~3번씩 섞어 넣어도 결과가 같다.
- 모든 지점의 스냅샷 + 나머지 리플레이 결과가 전체 리플레이와 같다(JSON 직렬화 왕복 포함).
- 50개 무작위 대화에서 이미 반영된 이벤트가 무작위 순서로 다시 와도 결과가 같다.
- 빈틈이 있으면 실패한다.

## 5. 복원 비용과 성능 설계

### 인덱스
| 단계 | 쿼리 | 인덱스 | 실측(이벤트 158만 건) |
|---|---|---|---|
| at → seq | `ORDER BY server_ts DESC, seq DESC LIMIT 1` | `(session_id, server_ts, seq)` Index Only Scan | 0.07 ms |
| 스냅샷 찾기 | `seq <= target ORDER BY seq DESC LIMIT 1` | PK `(session_id, seq)` 역방향 | 1행 |
| 리플레이 | `seq > a AND seq <= b ORDER BY seq` | PK 범위 스캔 | 200행 1.6 ms |

### 스냅샷 주기와 저장 형식
- **주기**: seq 가 `interval`(기본 100, `SNAPSHOT_INTERVAL`)의 배수인 지점과 세션 종료 시점. 워커 실행 타이밍과 무관한 결정적 위치라서, 어떤 시점을 복원해도 리플레이는 최대 `interval-1` 개다.
- **생성 비용**: 직전 스냅샷 + interval 개 리플레이. 비동기 워커가 만들므로 쓰기 지연에 영향이 없다.
- **형식**: `jsonb`(TOAST 압축). 사람이 읽을 수 있고 SQL 로 조사할 수 있다. 크기를 더 줄여야 하면 바이너리(Protobuf/Smile) + `bytea` 로 바꿀 수 있고, `state_version` 이 있어 형식 전환이 안전하다.
- **주기 튜닝 기준**: `chat_timeline_replayed_events_total` / 복원 횟수 = 평균 리플레이 길이. 복원 지연 p99 와 스냅샷 저장량 사이에서 주기를 정한다.

### 측정 결과와 한계
이벤트 5,000개 세션(메시지 약 4,200개, 수정 500개, 읽음 290개), 각 30회, 클라이언트 측 지연:

| 복원 지점 | AUTO(스냅샷+리플레이) | FULL(전체 리플레이) | AUTO 리플레이 수 |
|---|---|---|---|
| seq 5000 (head) | p50 18.5 ms | p50 18.5 ms | 0 (스냅샷 5000) |
| seq 2550 | p50 11.6 ms | p50 10.8 ms | 50 (스냅샷 2500) |
| seq 150 | p50 4.5 ms | p50 4.4 ms | 50 (스냅샷 100) |

**해석(솔직한 결론)**: 이 데이터에서는 스냅샷 이득이 거의 없다. 이유는 두 가지다.
1. **스냅샷 역직렬화 비용도 메시지 수에 비례한다.** 스냅샷이 "모든 메시지" 를 담고 있어서, 상태 크기 ≈ 이벤트 수인 대화형 데이터에서는 스냅샷을 읽는 비용과 이벤트를 읽는 비용이 비슷하다.
2. 벤치마크 세션은 일괄 삽입이라 이벤트가 힙에 **연속으로** 놓였고 캐시에 있었다. 실서비스처럼 여러 세션이 섞여 쓰이면(Q1: 200행에 200블록) 콜드 리플레이는 랜덤 I/O 가 되어 스냅샷의 상대적 이득이 커진다.

스냅샷이 확실히 이기는 경우는 다음과 같다. ① 상태보다 이벤트가 훨씬 많을 때(presence 깜빡임, READ, 수정이 많은 대화) ② 이벤트가 콜드 스토리지로 아카이빙됐을 때(최종 스냅샷만으로 복원) ③ 이벤트 열이 매우 길 때(수십만 건).

## 6. 개선 설계: 시간축 메시지 프로젝션

스냅샷이 메시지를 통째로 들고 다니는 문제를 없애는 다음 단계 설계다(미구현, 설계만).

```sql
-- 메시지의 "버전" 을 seq 구간으로 저장 (Temporal / SCD Type 2)
CREATE TABLE message_versions (
    session_id  uuid   NOT NULL,
    message_seq bigint NOT NULL,        -- 원본 MESSAGE 의 seq
    valid_from  bigint NOT NULL,        -- 이 버전이 생긴 이벤트 seq
    valid_to    bigint,                 -- 다음 버전(수정/삭제) seq, 현재 버전이면 NULL
    sender_id   varchar(64), content text, status varchar(16),
    PRIMARY KEY (session_id, message_seq, valid_from)
);
-- 시점 T 의 최근 N개 메시지
SELECT * FROM message_versions
 WHERE session_id = :sid AND valid_from <= :T AND (valid_to IS NULL OR valid_to > :T)
 ORDER BY message_seq DESC LIMIT :n;
```
- 스냅샷에는 **작은 상태(참여자, presence, 읽음 위치, 카운터)만** 남긴다. 그러면 스냅샷 크기가 메시지 수와 무관해진다.
- 메시지 목록은 위 쿼리 하나로 "그 시점의 최근 N개" 를 바로 얻는다. 리플레이가 필요 없다.
- 이 테이블도 이벤트에서 결정적으로 만들어지는 프로젝션이라 언제든 재구축할 수 있고, 지금의 outbox 워커가 그대로 갱신하면 된다.
- 비용은 쓰기 증폭(메시지 이벤트마다 1행, 수정 시 기존 행 `valid_to` 갱신)이다. 1:1 대화량이라면 충분히 감당할 수 있다.
