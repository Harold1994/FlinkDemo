package flink.streaming.statemachine.kafka;

import flink.streaming.statemachine.event.Event;
import flink.streaming.statemachine.event.EventType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class EventDeSerializer implements DeserializationSchema<Event>, SerializationSchema<Event> {
    private static final long serialVersionUID = 1L;
    @Override
    public Event deserialize(byte[] bytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        int address = buffer.getInt(0);
        int typeOrdinal = buffer.getInt(4);
        return new Event(EventType.values()[typeOrdinal], address);
    }

    @Override
    public boolean isEndOfStream(Event event) {
        return false;
    }

    @Override
    public byte[] serialize(Event event) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(0, event.sourceAddress());
        byteBuffer.putInt(4, event.type().ordinal());
        return byteBuffer.array();
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
