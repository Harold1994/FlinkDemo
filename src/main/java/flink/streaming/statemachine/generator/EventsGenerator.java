package flink.streaming.statemachine.generator;

import flink.streaming.statemachine.dfa.EventTypeAndState;
import flink.streaming.statemachine.dfa.State;
import flink.streaming.statemachine.event.Event;
import flink.streaming.statemachine.event.EventType;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import static org.apache.flink.util.Preconditions.checkArgument;

public class EventsGenerator {
    private final Random rnd;
    private final LinkedHashMap<Integer, State> states;
    private final double errorProb;

    public EventsGenerator() {
        this(0.0);
    }

    public EventsGenerator(double errorProb) {
        checkArgument(errorProb >= 0.0 && errorProb <= 1.0, "Invalid error probability");

        this.errorProb = errorProb;
        this.rnd = new Random();
        this.states = new LinkedHashMap<>();
    }

    public Event next(int minIP, int maxIp) {
        final double p = rnd.nextDouble();
        if (p * 1000 > states.size()) {
            // 创建一个新的状态机
            final int nextIp = rnd.nextInt(maxIp - minIP) + minIP;
            if (!states.containsKey(nextIp)) {
                EventTypeAndState eventTypeAndState = State.Initial.randomTransition(rnd);
                states.put(nextIp, eventTypeAndState.state);
                return new Event(eventTypeAndState.eventType, nextIp);
            } else {
                return next(minIP, maxIp);
            }
        } else {
            // 选择已有的状态机，更新它的状态
            int numToSkip = Math.min(20, rnd.nextInt(states.size()));
            Iterator<Map.Entry<Integer, State>> iter = states.entrySet().iterator();
            for (int i = numToSkip; i > 0; i--)
                iter.next();
            Map.Entry<Integer, State> entry = iter.next();
            State curState = entry.getValue();
            int address = entry.getKey();
            iter.remove();
            if (p < errorProb) {
                EventType event = curState.randomInvalidTransition(rnd);
                return new Event(event, address);
            } else {
                EventTypeAndState eventTypeAndState = curState.randomTransition(rnd);
                if (!eventTypeAndState.state.isTerminal())
                    states.put(address, eventTypeAndState.state);
                return new Event(eventTypeAndState.eventType, address);
            }

        }
    }

    @Nullable
    public Event nextInvalid() {
        final Iterator<Map.Entry<Integer, State>> iter = states.entrySet().iterator();
        if (iter.hasNext()) {
            final Map.Entry<Integer, State> entry = iter.next();
            State curState = entry.getValue();
            int address = entry.getKey();
            iter.remove();
            EventType eventType = curState.randomInvalidTransition(rnd);
            return new Event(eventType, address);
        } else
            return null;
    }

    public int numActiveEntries() {
        return states.size();
    }
}
