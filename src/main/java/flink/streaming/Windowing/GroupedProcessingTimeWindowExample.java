package flink.streaming.Windowing;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

public class GroupedProcessingTimeWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<Tuple2<Long, Long>> stream = env.addSource(new DataSource());
        stream.keyBy(0)
                .timeWindow(Time.of(2500, TimeUnit.MILLISECONDS), Time.of(500, TimeUnit.MILLISECONDS))
                .reduce(new SummingReducer())
                .addSink(new SinkFunction<Tuple2<Long, Long>>() {
                    @Override
                    public void invoke(Tuple2<Long, Long> value, Context context) throws Exception {
                    }
                });
        stream.print();
        env.execute();
    }

    private static class SummingReducer implements ReduceFunction<Tuple2<Long, Long>> {

        @Override
        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) throws Exception {
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        }
    }

    private static class DataSource implements ParallelSourceFunction<Tuple2<Long, Long>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> sourceContext) throws Exception {
            final long startTime = System.currentTimeMillis();
            final long numElements = 20000000;
            final long numKeys = 10000;
            long val = 1L;
            long count = 0L;
            while (running && val < numElements) {
                count++;
                sourceContext.collect(new Tuple2<>(val++, 1l));
                if (val > numKeys)
                    val = 1l;
            }
            final long endTime = System.currentTimeMillis();
            System.out.println("Took " + (endTime - startTime) + " msecs for " + numElements + " values");
    }

    @Override
    public void cancel() {
        running = false;
    }
}
}
