package flink.streaming.statemachine.dfa;

import flink.streaming.statemachine.event.EventType;

import java.util.Random;

public enum State {
    Terminal,
    InvalidTransition,
    Z(new Transition(EventType.g, Terminal, 1.0f)),
    Y(new Transition(EventType.e, Z, 1.0f)),
    X(new Transition(EventType.d, Z, 0.8f), new Transition(EventType.b, Y, 0.2f)),
    W(new Transition(EventType.b, Y, 1.0f)),
    Initial(new Transition(EventType.b, W, 0.6f), new Transition(EventType.c, X, 0.4f));

    private final Transition[] transitions;

    State(Transition... transitions) {
        this.transitions = transitions;
    }

    public boolean isTerminal() {
        return transitions.length == 0;
    }

    public State transition(EventType evt) {
        for (Transition t : transitions) {
            if (t.getEventType() == evt)
                return t.getTargetState();
        }
        return InvalidTransition;
    }

    public EventTypeAndState randomTransition(Random rnd) {
        if (isTerminal())
            throw new RuntimeException("Cant transition from state " + name());
        else {
            final float p = rnd.nextFloat();
            float mass = 0.0f;
            Transition transition = null;
            for (Transition t : transitions) {
                mass += t.getProb();
                if (p <= mass) {
                    transition = t;
                    break;
                }
            }
            assert transitions != null;
            return new EventTypeAndState(transition.getEventType(), transition.getTargetState());
        }
    }

    public EventType randomInvalidTransition(Random rnd) {
        while (true) {
            EventType candidate = EventType.values()[rnd.nextInt(EventType.values().length)];
            if (transition(candidate) == InvalidTransition)
                return candidate;
        }
    }


}
