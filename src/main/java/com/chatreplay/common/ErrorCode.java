package com.chatreplay.common;

import org.springframework.http.HttpStatus;

public enum ErrorCode {
    INVALID_REQUEST(HttpStatus.BAD_REQUEST, false),
    MISSING_USER(HttpStatus.UNAUTHORIZED, false),
    SESSION_NOT_FOUND(HttpStatus.NOT_FOUND, false),
    SESSION_ENDED(HttpStatus.CONFLICT, false),
    SESSION_FULL(HttpStatus.CONFLICT, false),
    NOT_INVITED(HttpStatus.FORBIDDEN, false),
    NOT_A_PARTICIPANT(HttpStatus.FORBIDDEN, false),
    EVENT_TYPE_NOT_ALLOWED(HttpStatus.BAD_REQUEST, false),
    /** 수정/삭제 대상 메시지가 아직 도착하지 않았을 수 있다(순서 뒤바뀜). 클라이언트는 재시도한다. */
    TARGET_NOT_FOUND(HttpStatus.UNPROCESSABLE_CONTENT, true),
    TARGET_NOT_OWNED(HttpStatus.FORBIDDEN, false),
    TARGET_DELETED(HttpStatus.CONFLICT, false),
    NO_STATE_AT(HttpStatus.NOT_FOUND, false),
    SERVICE_UNAVAILABLE(HttpStatus.SERVICE_UNAVAILABLE, true);

    private final HttpStatus status;
    private final boolean retryable;

    ErrorCode(HttpStatus status, boolean retryable) {
        this.status = status;
        this.retryable = retryable;
    }

    public HttpStatus status() {
        return status;
    }

    public boolean retryable() {
        return retryable;
    }
}
