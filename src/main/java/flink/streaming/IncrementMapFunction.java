package flink.streaming;

import org.apache.flink.api.common.functions.MapFunction;

public class IncrementMapFunction implements MapFunction<Long, Long> {
    @Override
    public Long map(Long aLong) throws Exception {
        return aLong + 1;
    }
}
