package flink.batch;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class CombinableGroupReducerDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        DataSet<Tuple2<String, Integer>> elements = env.fromElements(new Tuple2<>("java",3), new Tuple2<>("scala", 2), new Tuple2<>("java", 1));

        elements.groupBy(0)
                .reduceGroup(new MyCombinableGroupReducer())
                .print();

//        env.execute();
    }

    public static class MyCombinableGroupReducer implements GroupReduceFunction<Tuple2<String, Integer>, String>,
            GroupCombineFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        @Override
        public void combine(Iterable<Tuple2<String, Integer>> in, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String key = null;
            int sum = 0;

            for (Tuple2<String, Integer> curr : in) {
                key = curr.f0;
                sum += curr.f1;
            }
            // emit tuple with key and sum
            collector.collect(new Tuple2<>(key, sum));
        }

        @Override
        public void reduce(Iterable<Tuple2<String, Integer>> in, Collector<String> collector) throws Exception {
            String key = null;
            int sum = 0;

            for (Tuple2<String, Integer> curr : in) {
                key = curr.f0;
                sum += curr.f1;
            }
            collector.collect(key + "_" + sum);
        }
    }
}
