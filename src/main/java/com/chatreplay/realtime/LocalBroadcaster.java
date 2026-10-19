package com.chatreplay.realtime;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** 단일 인스턴스용: 같은 프로세스의 연결에만 전달한다. */
@Component
@ConditionalOnProperty(name = "chat.broadcast.mode", havingValue = "local", matchIfMissing = true)
public class LocalBroadcaster implements SessionBroadcaster {

    private final LocalDispatcher dispatcher;

    public LocalBroadcaster(LocalDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void publish(BroadcastEnvelope envelope) {
        dispatcher.dispatch(envelope);
    }
}
