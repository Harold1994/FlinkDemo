package flink.streaming.statemachine.dfa;

import flink.streaming.statemachine.event.EventType;
import java.io.Serializable;

public class Transition implements Serializable {
    private static final long serialVersionUID = 1L;
    private final EventType eventType;
    private final State targetState;
    private final float prob;

    public Transition(EventType eventType, State targetState, float prob) {
        this.eventType = eventType;
        this.targetState = targetState;
        this.prob = prob;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public EventType getEventType() {
        return eventType;
    }

    public State getTargetState() {
        return targetState;
    }

    public float getProb() {
        return prob;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transition that = (Transition) o;
        return Float.compare(that.prob, prob) == 0 &&
                eventType == that.eventType &&
                targetState == that.targetState;
    }

    @Override
    public int hashCode() {
        int code = 31 * eventType.hashCode() + targetState.hashCode();
        return 31 * code + (prob != +0.0f ? Float.floatToIntBits(prob) : 0);
    }

    @Override
    public String toString() {
        return "--[" +
                "eventType=" + eventType +
                ", targetState=" + targetState +
                ", prob=" + prob +
                ']';
    }
}
