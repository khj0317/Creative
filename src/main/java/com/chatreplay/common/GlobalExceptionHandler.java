package com.chatreplay.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/** 모든 오류를 RFC 9457 Problem Details + code/retryable 필드로 응답한다. */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    public ProblemDetail handleApi(ApiException e) {
        return problem(e.code(), e.getMessage());
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, HttpMessageNotReadableException.class,
            MethodArgumentTypeMismatchException.class, IllegalArgumentException.class})
    public ProblemDetail handleBadRequest(Exception e) {
        return problem(ErrorCode.INVALID_REQUEST, e.getMessage());
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    public ProblemDetail handleMissingHeader(MissingRequestHeaderException e) {
        return problem(ErrorCode.MISSING_USER, "X-User-Id 헤더가 필요합니다.");
    }

    /** DB 커넥션 고갈/장애: 503 + retryable 로 응답해 클라이언트가 백오프 후 재시도하게 한다. */
    @ExceptionHandler({CannotGetJdbcConnectionException.class, DataAccessResourceFailureException.class,
            QueryTimeoutException.class})
    public ProblemDetail handleDbUnavailable(Exception e) {
        log.error("database unavailable: {}", e.getMessage());
        ProblemDetail pd = problem(ErrorCode.SERVICE_UNAVAILABLE, "일시적으로 저장소를 사용할 수 없습니다. 잠시 후 다시 시도하세요.");
        pd.setProperty("retryAfterSeconds", 2);
        return pd;
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnknown(Exception e) {
        log.error("unhandled error", e);
        ProblemDetail pd = ProblemDetail.forStatus(HttpStatus.INTERNAL_SERVER_ERROR);
        pd.setProperty("code", "INTERNAL_ERROR");
        pd.setProperty("retryable", true);
        return pd;
    }

    private static ProblemDetail problem(ErrorCode code, String detail) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(code.status(), detail);
        pd.setProperty("code", code.name());
        pd.setProperty("retryable", code.retryable());
        return pd;
    }
}
