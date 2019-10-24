package flink.streaming.statemachine.event;

import flink.streaming.statemachine.dfa.State;

import java.util.Objects;

public class Alert {
    private final int address;
    private final State state;
    private final EventType transition;

    public Alert(int address, State state, EventType transition) {
        this.address = address;
        this.state = state;
        this.transition = transition;
    }

    public int getAddress() {
        return address;
    }

    public State getState() {
        return state;
    }

    public EventType getTransition() {
        return transition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Alert alert = (Alert) o;
        return address == alert.address &&
                state == alert.state &&
                transition == alert.transition;
    }

    @Override
    public int hashCode() {
        int code = 31 * address + state.hashCode();
        return 31 * code + transition.hashCode();
    }

    @Override
    public String toString() {
        return "Alert " +
                "address: " + address +
                ", state: " + state +
                ", transition: " + transition;
    }
}
