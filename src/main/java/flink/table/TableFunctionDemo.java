package flink.table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

public class TableFunctionDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment bEnv = BatchTableEnvironment.create(env);

        bEnv.registerFunction("split", new CustomTypeSplit(" "));

        DataSet<String> ds = env.fromElements(
                "hello world",
                "Hello harold"
        );

        Table myTable = bEnv.fromDataSet(ds, "a");
        Table res1 = myTable.joinLateral("split(a) as (word, length)")
                .select("a, word, length");
        Table res2 = myTable.leftOuterJoinLateral("split(a) as (word, length)")
                .select("a, word, length");

        bEnv.toDataSet(res1, Row.class).print();
        bEnv.toDataSet(res2, Row.class).print();

        bEnv.registerTable("myTable", myTable);
        Table res3 = bEnv.sqlQuery("select a, word, length from myTable, LATERAL TABLE(split(a)) as T(word, length)");
        Table res4 = bEnv.sqlQuery("SELECT a, word, length FROM myTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE");

        bEnv.toDataSet(res3, Row.class).print();
        bEnv.toDataSet(res4, Row.class).print();
    }
}
