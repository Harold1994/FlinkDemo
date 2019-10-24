package flink.streaming.statemachine.dfa;

import flink.streaming.statemachine.event.EventType;

public class EventTypeAndState {
    public final EventType eventType;
    public final State state;

    public EventTypeAndState(EventType eventType, State state) {
        this.eventType = eventType;
        this.state = state;
    }
}
