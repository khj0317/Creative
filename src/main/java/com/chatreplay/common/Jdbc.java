package com.chatreplay.common;

import java.sql.Timestamp;
import java.time.Instant;

/** JDBC 시간 변환 도우미. DB 컬럼은 모두 timestamptz 이고 애플리케이션은 Instant(UTC)만 쓴다. */
public final class Jdbc {

    private Jdbc() {
    }

    public static Timestamp ts(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    public static Instant instant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }
}
