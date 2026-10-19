-- =====================================================================
-- 쓰기 모델(Write side)
--   sessions / session_participants : 명령 검증에 필요한 "현재 상태" (동기 갱신)
--   session_events                  : 단일 진실 원천(Source of Truth), append-only
-- 읽기 모델(Read side)
--   session_snapshots               : 특정 seq 시점의 상태 (복원 가속)
--   session_projections             : 목록/요약용 비동기 프로젝션
-- 비동기 처리
--   projection_outbox / projection_dlq : Transactional Outbox + DLQ
-- 운영
--   server_instances                : 인스턴스 하트비트 (장애 인스턴스의 presence 정리)
-- =====================================================================

CREATE TABLE sessions (
    id             uuid         PRIMARY KEY,
    title          varchar(200),
    status         varchar(16)  NOT NULL,              -- WAITING | ACTIVE | INTERRUPTED | COMPLETED
    created_by     varchar(64)  NOT NULL,
    invitees       varchar(64)[] NOT NULL DEFAULT '{}', -- 비어 있으면 누구나 참여 가능(최대 2명)
    created_at     timestamptz  NOT NULL,
    ended_at       timestamptz,
    last_seq       bigint       NOT NULL DEFAULT 0,     -- 세션별 단조 증가 시퀀스의 현재 값
    last_event_at  timestamptz  NOT NULL                -- server_ts 단조성 보장용
);

-- GET /sessions 핫패스: 상태 + 기간 필터, created_at DESC 키셋 페이지네이션
CREATE INDEX idx_sessions_status_created ON sessions (status, created_at DESC, id DESC);
CREATE INDEX idx_sessions_created       ON sessions (created_at DESC, id DESC);

CREATE TABLE session_participants (
    session_id         uuid        NOT NULL REFERENCES sessions (id),
    user_id            varchar(64) NOT NULL,
    state              varchar(16) NOT NULL,   -- JOINED | LEFT
    presence           varchar(16) NOT NULL,   -- ONLINE | OFFLINE
    joined_at          timestamptz NOT NULL,
    left_at            timestamptz,
    last_seen_at       timestamptz NOT NULL,
    connection_id      varchar(64),            -- 현재 유효한 WebSocket 연결 (없으면 NULL)
    connected_instance varchar(64),            -- 해당 연결을 들고 있는 서버 인스턴스
    PRIMARY KEY (session_id, user_id)
);

-- GET /sessions?participant= 필터: user_id 로 세션을 찾는다
CREATE INDEX idx_participants_user ON session_participants (user_id, session_id);
-- 장애 인스턴스 정리(reaper) 핫패스: 연결 중인 참여자만 부분 인덱스
CREATE INDEX idx_participants_instance ON session_participants (connected_instance)
    WHERE connection_id IS NOT NULL;

CREATE TABLE session_events (
    session_id      uuid        NOT NULL REFERENCES sessions (id),
    seq             bigint      NOT NULL,      -- 세션 내 정렬의 유일한 기준(서버 부여)
    event_id        uuid        NOT NULL,
    client_event_id varchar(80) NOT NULL,      -- 멱등 키(클라이언트 생성, 서버 이벤트는 결정적 키)
    type            varchar(32) NOT NULL,
    actor_id        varchar(64) NOT NULL,
    payload         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    client_ts       timestamptz,               -- 참고용(정렬에 사용하지 않음)
    server_ts       timestamptz NOT NULL,      -- seq 와 함께 단조 증가(비감소)
    PRIMARY KEY (session_id, seq)
);

-- 중복 이벤트 방지: 같은 사용자가 같은 멱등 키로 재전송하면 저장되지 않는다
CREATE UNIQUE INDEX uq_events_idempotency ON session_events (session_id, actor_id, client_event_id);
-- timeline?at= 시각 -> seq 변환 (Index Only Scan)
CREATE INDEX idx_events_session_ts ON session_events (session_id, server_ts, seq);
-- 수정/삭제 대상 검증: 특정 메시지(seq)에 대한 수정/삭제 이벤트 조회
CREATE INDEX idx_events_message_ops ON session_events (session_id, ((payload ->> 'targetSeq')::bigint))
    WHERE type IN ('MESSAGE_EDITED', 'MESSAGE_DELETED');

CREATE TABLE session_snapshots (
    session_id    uuid        NOT NULL REFERENCES sessions (id),
    seq           bigint      NOT NULL,         -- 이 seq 까지 반영된 상태
    state_version int         NOT NULL,         -- 상태 스키마 버전(호환 안 되면 무시하고 전체 리플레이)
    state         jsonb       NOT NULL,
    event_count   int         NOT NULL,
    created_at    timestamptz NOT NULL,
    PRIMARY KEY (session_id, seq)               -- "seq <= ? ORDER BY seq DESC LIMIT 1" 백워드 스캔
);

CREATE TABLE session_projections (
    session_id           uuid        PRIMARY KEY REFERENCES sessions (id),
    projected_seq        bigint      NOT NULL DEFAULT 0,   -- 체크포인트: 여기까지 반영됨(멱등성의 기준)
    message_count        int         NOT NULL DEFAULT 0,
    last_message_seq     bigint,
    last_message_at      timestamptz,
    last_message_preview varchar(200),
    last_snapshot_seq    bigint      NOT NULL DEFAULT 0,
    updated_at           timestamptz NOT NULL
);

CREATE TABLE projection_outbox (
    session_id      uuid        PRIMARY KEY REFERENCES sessions (id), -- 세션당 1행으로 신호를 합친다(coalescing)
    target_seq      bigint      NOT NULL,
    attempts        int         NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL,
    locked_by       varchar(64),
    locked_until    timestamptz,
    last_error      text,
    enqueued_at     timestamptz NOT NULL
);

CREATE INDEX idx_outbox_due ON projection_outbox (next_attempt_at);

CREATE TABLE projection_dlq (
    id          bigserial   PRIMARY KEY,
    session_id  uuid        NOT NULL REFERENCES sessions (id),
    target_seq  bigint      NOT NULL,
    attempts    int         NOT NULL,
    last_error  text,
    failed_at   timestamptz NOT NULL,
    resolved_at timestamptz
);

CREATE INDEX idx_dlq_unresolved ON projection_dlq (failed_at) WHERE resolved_at IS NULL;

CREATE TABLE server_instances (
    instance_id  varchar(64) PRIMARY KEY,
    started_at   timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL
);
