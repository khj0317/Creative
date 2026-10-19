package com.chatreplay.config;

/** 이 서버 인스턴스의 식별자. 하트비트, presence 정리, 워커 lease 소유자 표시에 쓴다. */
public record InstanceId(String value) {
}
