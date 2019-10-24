package flink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class IncrementFlatmapFunction implements FlatMapFunction<Long, Long> {
    @Override
    public void flatMap(Long aLong, Collector<Long> collector) throws Exception {
        for (int i = 0; i<5; i++) {
            collector.collect(aLong + i);
        }
    }
}
