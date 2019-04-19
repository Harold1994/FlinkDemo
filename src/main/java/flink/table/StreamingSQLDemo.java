package flink.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

public class StreamingSQLDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment sEnv = StreamTableEnvironment.create(env);
        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)));

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));

        Table tableA = sEnv.fromDataStream(orderA, "user, producer, count");
        sEnv.registerDataStream("OrderB", orderB, "user, producer, count");

        Table result = sEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL SELECT * FROM OrderB WHERE amount < 2");

        sEnv.toAppendStream(result, Order.class).print();
        env.execute();
    }

    public static class Order{
        long user;
        String product;
        int count;

        public Order(long user, String product, int count) {
            this.user = user;
            this.product = product;
            this.count = count;
        }

        public Order() {
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
