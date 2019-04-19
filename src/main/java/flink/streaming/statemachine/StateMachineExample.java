package flink.streaming.statemachine;


import flink.streaming.statemachine.dfa.State;
import flink.streaming.statemachine.event.Alert;
import flink.streaming.statemachine.event.Event;
import flink.streaming.statemachine.generator.EventsGeneratorSource;
import flink.streaming.statemachine.kafka.EventDeSerializer;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class StateMachineExample {
    public static void main(String[] args) throws Exception {
        System.out.println("Usage with built-in data generator: StateMachineExample [--error-rate <probability-of-invalid-transition>] [--sleep <sleep-per-record-in-ms>]");
        System.out.println("Usage with Kafka: StateMachineExample --kafka-topic <topic> [--brokers <brokers>]");
        System.out.println("Options for both the above setups: ");
        System.out.println("\t[--backend <file|rocks>]");
        System.out.println("\t[--checkpoint-dir <filepath>]");
        System.out.println("\t[--async-checkpoints <true|false>]");
        System.out.println("\t[--incremental-checkpoints <true|false>]");
        System.out.println("\t[--output <filepath> OR null for stdout]");
        System.out.println();

        final SourceFunction<Event> source;
        final ParameterTool params = ParameterTool.fromArgs(args);
        if (params.has("kafka-topic")) {
            // set up the Kafka reader
            String kafkaTopic = params.get("kafka-topic");
            String brokers = params.get("brokers", "localhost:9092");

            System.out.printf("Reading from kafka topic %s @ %s\n", kafkaTopic, brokers);
            System.out.println();

            Properties kafkaProps = new Properties();
            kafkaProps.setProperty("bootstrap.servers", brokers);

            FlinkKafkaConsumer<Event> kafka = new FlinkKafkaConsumer<>(kafkaTopic, new EventDeSerializer(), kafkaProps);
            kafka.setStartFromLatest();
            kafka.setCommitOffsetsOnCheckpoints(false);
            source = kafka;
        }
        else {
            double errorRate = params.getDouble("error-rate", 0.0);
            int sleep = params.getInt("sleep", 1);

            System.out.printf("Using standalone source with error rate %f and sleep delay %s millis\n", errorRate, sleep);
            System.out.println();

            source = new EventsGeneratorSource(errorRate, sleep);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);
        final String stateBackend = params.get("backend", "memory");
        if ("file".equals(stateBackend)) {
            final String checkpointDir = params.get("checkpoint-dir");
            boolean asyncCheckpoints = params.getBoolean("async-checkpoints", false);
            env.setStateBackend(new FsStateBackend(checkpointDir, asyncCheckpoints));
        }

        final String outputFile = params.get("output");
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Event> events = env.addSource(source);
        DataStream<Alert> alerts = events
                .keyBy(Event::sourceAddress)
                .flatMap(new StateMachineMapper());
        if (outputFile == null) {
            alerts.print();
        } else {
            alerts
                    .writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE)
                    .setParallelism(1);
        }

        // trigger program execution
        env.execute("State machine job");
    }

    private static class StateMachineMapper extends RichFlatMapFunction<Event, Alert> {
        private ValueState<State> currentState;
        @Override
        public void open(Configuration parameters) throws Exception {
            currentState = getRuntimeContext().getState(new ValueStateDescriptor<State>("state", State.class));
        }

        @Override
        public void flatMap(Event event, Collector<Alert> collector) throws Exception {
            State state = currentState.value();
            if (state == null)
                state = State.Initial;

            State nextState = state.transition(event.type());
            if (nextState == state.InvalidTransition)
                collector.collect(new Alert(event.sourceAddress(), state, event.type()));
            else if (nextState.isTerminal()) {
                // we reached a terminal state, clean up the current state
                currentState.clear();
            }
            else {
                // remember the new state
                currentState.update(nextState);
            }
        }
    }
}
