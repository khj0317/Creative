package com.chatreplay;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class ChatReplayApplication {

    public static void main(String[] args) {
        SpringApplication.run(ChatReplayApplication.class, args);
    }
}
