package flink.table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

public class SetOperationDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        DataSet<Tuple3<Integer, String, String>> ds = env.fromElements(
                new Tuple3<>(0, "1", "1s"),
                new Tuple3<>(1, "a", "1w"),
                new Tuple3<>(2, "d", "1qa"),
                new Tuple3<>(3, "fdd", "1d"),
                new Tuple3<>(4, "ss", "1gf"),
                new Tuple3<>(5, "dd", "1k")
        );
        DataSet<Integer> ds2 = env.fromElements(1,2,3,4,5);

        Table left = tableEnv.fromDataSet(ds, "a, b, c");
        Table right = tableEnv.fromDataSet(ds2, "q");

        Table res = left.select("a,b,c").where("a.in (" + right + ")");
        tableEnv.toDataSet(res, Row.class).print();
    }
}
