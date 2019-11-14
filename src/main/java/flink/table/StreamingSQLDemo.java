package flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class StreamingSQLDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment sEnv = StreamTableEnvironment.create(env);
        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1, "beer", 3),
                new Order(6, "diaper", 4),
                new Order(12, "beer", 3),
                new Order(15, "diaper", 4),
                new Order(11, "beer", 3),
                new Order(90, "diaper", 4),
                new Order(3, "rubber", 2)));

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2, "pen", 3),
                new Order(2, "rubber", 3),
                new Order(2, "rubber", 4),
                new Order(4, "beer", 1),
                new Order(4, "beer", 1),
                new Order(2, "beer", 1),
                new Order(2, "pen", 3),
                new Order(2, "rubber", 3),
                new Order(2, "rubber", 4),
                new Order(4, "beer", 1)
                )
        );

        Table tableA = sEnv.fromDataStream(orderA, "count,product,user");
        sEnv.registerDataStream("OrderB", orderB, "count,product,user");

//        Table result = sEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE `count` > 2 UNION ALL SELECT * FROM OrderB WHERE `count` < 2");
//        sEnv.toAppendStream(result, Order.class).print();

        Table result2 = sEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE `count` BETWEEN SYMMETRIC 11 AND 15");
        sEnv.toAppendStream(result2, Order.class).print();



//        TableSink csvSink = new CsvTableSink("/Users/harold/Documents/Study/FlinkDemo/out", ",");
//        String [] fieldName = {"product", "user"};
//        TypeInformation[] fieldTypes = {Types.STRING, Types.LONG};
//        sEnv.registerTableSink("RubberOrders", fieldName, fieldTypes, csvSink);
//        sEnv.sqlUpdate("insert into RubberOrders SELECT product,user from OrderB where product LIKE '%ee%'");
//

        env.execute();
    }
}
