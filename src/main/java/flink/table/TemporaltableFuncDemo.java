package flink.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;

import java.util.ArrayList;
import java.util.List;


public class TemporaltableFuncDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<Tuple2<String, Long>> ratesHistoryData = new ArrayList<>();
        ratesHistoryData.add(Tuple2.of("US Dollar", 102L));
        ratesHistoryData.add(Tuple2.of("Euro", 114L));
        ratesHistoryData.add(Tuple2.of("Yen", 1L));
        ratesHistoryData.add(Tuple2.of("Euro", 116L));
        ratesHistoryData.add(Tuple2.of("Euro", 119L));

        DataStream<Tuple2<String, Long>> rateHistoryStream = env.fromCollection(ratesHistoryData);
        Table rateHistory  = tEnv.fromDataStream(rateHistoryStream, "r_currency ,r_rate, r_proctime.proctime");

        tEnv.registerTable("RateHistory", rateHistory);

        TemporalTableFunction rates = rateHistory.createTemporalTableFunction("r_proctime", "r_currency");
        tEnv.registerFunction("Rates", rates);

    }
}
