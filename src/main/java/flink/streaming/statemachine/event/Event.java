package flink.streaming.statemachine.event;

public class Event {
    private final EventType type;
    private final int sourceAddress;

    public Event(EventType type, int sourceAddress) {
        this.type = type;
        this.sourceAddress = sourceAddress;
    }

    public EventType type() {
        return type;
    }

    public int sourceAddress() {
        return sourceAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        else if (o == null || getClass() != o.getClass()) return false;
        else {
            final Event that = (Event) o;
            return this.sourceAddress == that.sourceAddress && this.type == that.type;
        }
    }

    @Override
    public int hashCode() {
        return 31 * type.hashCode() + sourceAddress;
    }

    @Override
    public String toString() {
        return "Event " + formatAddress(sourceAddress) + " : " + type.name();
    }

    public static String formatAddress(int address) {
        int b1 = (address >>> 24) & 0xff;
        int b2 = (address >>> 16) & 0xff;
        int b3 = (address >>> 8)  & 0xff;
        int b4 =  address         & 0xff;
        return "" + b1 + "." + b2 + "." + b3 + "." + b4;
    }
}
