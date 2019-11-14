package flink.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class ExplainDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStream<Tuple2<Integer, String>> stream1 = env.fromElements(new Tuple2<>(1, "hello"));
        DataStream<Tuple2<Integer, String>> stream2 = env.fromElements(new Tuple2<>(1, "hello"));

        Table table1 = tenv.fromDataStream(stream1, "count, word");
        Table table2 = tenv.fromDataStream(stream2, "count, word");

        Table table = table1
                .where("LIKE(word, 'F%')")
                .unionAll(table2)
                .select("*");

        String explanation  = tenv.explain(table);
        System.out.println(explanation);

        tenv.toAppendStream(table, Row.class).print();
        env.execute();
    }
}
