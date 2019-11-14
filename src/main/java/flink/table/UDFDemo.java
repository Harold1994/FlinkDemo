package flink.table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

public class UDFDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment bEnv = BatchTableEnvironment.create(env);

        bEnv.registerFunction("hashCode", new HashCode(10));
        
        DataSet<String> ds = env.fromElements(
                "str",
                "str2"
        );

        Table myTable = bEnv.fromDataSet(ds, "string");
        bEnv.registerTable("myTable", myTable);

        Table res1 = myTable.select("string, string.hashCode(), hashCode(string)");
        Table res2 = bEnv.sqlQuery("select string, hashCode(string) from myTable");

        bEnv.toDataSet(res1, Row.class).print();
        bEnv.toDataSet(res2, Row.class).print();
    }
}
