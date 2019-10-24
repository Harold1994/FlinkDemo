package flink.streaming.statemachine.kafka;

import akka.serialization.ByteArraySerializer;
import flink.streaming.statemachine.event.Event;
import flink.streaming.statemachine.generator.StandaloneThreadedGenerator;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaStandaloneGenerator extends StandaloneThreadedGenerator {
    public static final String BROKER_ADDRESS = "localhost:9092";
    public static final String TOPIC = "flink-demo-topic-1";
    public static final int NUM_PARTITIONS = 1;

    public static void main(String[] args) {

    }

    private class KafkaCollector implements Collector<Event>, AutoCloseable {
        private final KafkaProducer<Object, byte[]> producer;
        private final EventDeSerializer serializer;
        private final String topic;
        private final int partition;

        public KafkaCollector(EventDeSerializer serializer, String topic, int partition) {
            this.serializer = serializer;
            this.topic = topic;
            this.partition = partition;

            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_ADDRESS);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
            this.producer = new KafkaProducer<>(properties);
        }

        @Override
        public void collect(Event event) {
            byte[] serialized = serializer.serialize(event);
            producer.send(new ProducerRecord<>(topic, partition, null, serialized));
        }

        @Override
        public void close() {
            producer.close();
        }

    }

}
