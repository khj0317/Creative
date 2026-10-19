package com.chatreplay.event;

import java.time.Instant;
import java.util.Map;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

@Schema(description = "클라이언트가 보내는 이벤트 1건")
public record EventRequest(
        @Schema(description = "멱등 키(클라이언트 생성 UUID 권장). 재전송 시 같은 값을 보내야 한다.", example = "c1f3a2e0-7b1d-4c55-9f0e-2a9d3c1b7e01")
        @NotBlank @Size(max = 80) String clientEventId,
        @Schema(description = "MESSAGE | MESSAGE_EDITED | MESSAGE_DELETED | READ | JOIN | LEAVE")
        @NotNull EventType type,
        @Schema(description = "MESSAGE: {content} / MESSAGE_EDITED: {targetSeq, content} / MESSAGE_DELETED: {targetSeq} / READ: {upToSeq}",
                example = "{\"content\": \"안녕하세요\"}")
        Map<String, Object> payload,
        @Schema(description = "클라이언트 시각(참고용, 정렬에 쓰지 않음)")
        Instant clientTs) {
}
