package flink.streaming.statemachine;

import flink.streaming.statemachine.event.Event;
import flink.streaming.statemachine.generator.EventsGeneratorSource;
import flink.streaming.statemachine.kafka.EventDeSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class KafkaEventsGeneratorJob {
    public static void main(String[] args) throws Exception {
        ParameterTool param = ParameterTool.fromArgs(args);
        double errorRate = param.getDouble("error-rate", 0.0);
        int sleep = param.getInt("sleep", 1);
        String kafkaTopic = param.get("kafka-topic");
        String brokers = param.get("brokers", "localhost:9092");
        System.out.printf("Generating events to Kafka with standalone source with error rate %f and sleep delay %s millis\n", errorRate, sleep);
        System.out.println();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new EventsGeneratorSource(errorRate, sleep))
                .addSink(new FlinkKafkaProducer<Event>(brokers, kafkaTopic, new EventDeSerializer()));
        env.execute("\"State machine example Kafka events generator job\"");
    }
}
