package flink.table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.Over;
import org.apache.flink.types.Row;

public class GroupWindowDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        DataSet<Tuple3<Long, String, Long>> ds = env.fromElements(
                new Tuple3<>(1l,"hello", 1l),
                new Tuple3<>(2l,"hello", 3l),
                new Tuple3<>(3l,"world", 4l),
                new Tuple3<>(4l,"hello", 5l),
                new Tuple3<>(5l,"end", 6l),
                new Tuple3<>(5l,"hello", 14l),
                new Tuple3<>(6l,"world", 15l),
                new Tuple3<>(8l,"hello", 34l),
                new Tuple3<>(15l,"stop", 35l),
                new Tuple3<>(16l,"stop", 66l),
                new Tuple3<>(16l,"world", 68l),
        new Tuple3<>(16l,"stop", 1l)
        );

        Table table = tEnv.fromDataSet(ds, "long,word,times");

        Table res = table
                .window(Tumble.over("3.rows").on("long").as("w"))
                .groupBy("w, word")
                .select("word,times.sum");

        tEnv.toDataSet(res, Row.class).print();

//        System.out.println("-------------------------------------------------");
//        Table res2 = table
//                .window(Slide.over("10.rows").every("3.rows").on("long").as("w"))
//                .groupBy("w, word")
//                .select("word,times.sum");
//
//        tEnv.toDataSet(res2, Row.class).print();

        //TableException: Over-windows for batch tables are currently not supported.
//        System.out.println("-------------------------------------------------");
//        Table res3 = table
//                .window(Over.partitionBy("long").orderBy("times").preceding("3.rows").as("w"))
//                .select("word,times.sum");
//
//        tEnv.toDataSet(res3, Row.class).print();
    }
}
