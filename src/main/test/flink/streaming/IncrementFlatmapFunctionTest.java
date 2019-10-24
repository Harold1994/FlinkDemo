package flink.streaming;

import org.apache.flink.streaming.api.scala.function.StatefulFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;


import static org.junit.Assert.*;

public class IncrementFlatmapFunctionTest {

    @Test
    public void flatMap() throws Exception {
        IncrementFlatmapFunction incr = new IncrementFlatmapFunction();

//        Collector<Long> collector = mock(Collector.class);

//        incr.flatMap(2L, collector);
//        Mockito.
    }
}