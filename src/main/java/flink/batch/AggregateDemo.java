package flink.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple3;

public class AggregateDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        DataSet<Tuple3<Integer, String, Double>> in = env.fromElements(
                new Tuple3<>(1,"java",0.34d),
                new Tuple3<>(2,"scala", 8.76d),
                new Tuple3<>(4,"java",0.34d),
                new Tuple3<>(2,"c", 8.66d),
                new Tuple3<>(1,"java",7.34d),
                new Tuple3<>(3,"c", 11.76d)
        );

        in.groupBy(1)
                .minBy(0, 2)
                .print();

        DataSet<Tuple3<Integer, String, Double>> out = in
                .groupBy((1))
                .aggregate(Aggregations.SUM, 0)
                .and(Aggregations.MIN, 2);

        out.print();
    }
}
