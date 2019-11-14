package flink.table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

public class AggFuncDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment bEnv = BatchTableEnvironment.create(env);

        DataSet<Tuple3<String, Long, Integer>> ds = env.fromElements(
                Tuple3.of("A",13l, 1),
                Tuple3.of("B",20l, 2),
                Tuple3.of("A",10l, 3),
                Tuple3.of("C",13l, 3),
                Tuple3.of("B",20l, 2),
                Tuple3.of("A",10l, 1)
        );

        bEnv.registerDataSet("myTable", ds, "acc, points,level");
        bEnv.registerFunction("wAvg", new WeightedAvg());

        Table res = bEnv.sqlQuery("select acc, wAvg(points, level) from myTable group by acc");
        bEnv.toDataSet(res, Row.class).print();
    }
}
