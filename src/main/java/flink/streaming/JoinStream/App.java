package flink.streaming.JoinStream;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class App {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map props = new HashMap<String, String>();
        props.put("topic.rate", "rate");
        props.put("topic.order", "order");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");
        ParameterTool parameterTool = ParameterTool.fromMap(props);

        FlinkKafkaConsumer orderConsumer = new FlinkKafkaConsumer(parameterTool.getRequired("topic.order"), new DeserializationSchema() {
            @Override
            public Tuple5<Long, String, Integer, String, Integer> deserialize(byte[] bytes) throws IOException {
                String[] res = new String(bytes).split(",");
                Long timeStamp = Long.valueOf(res[0]);
                String catlog = res[1];
                Integer subcat = Integer.valueOf(res[2]);
                String dm = res[3];
                Integer price = Integer.valueOf(res[4]);
                return Tuple5.of(timeStamp, catlog, subcat, dm, price);
            }

            @Override
            public boolean isEndOfStream(Object o) {
                return false;
            }

            @Override
            public TypeInformation getProducedType() {
                return TypeInformation.of(new TypeHint<Tuple5<Long, String, Integer, String, Integer>>() {
                });
            }
        }, parameterTool.getProperties());

        FlinkKafkaConsumer rateConsumer = new FlinkKafkaConsumer(parameterTool.getRequired("topic.rate"), new DeserializationSchema() {
            @Override
            public Tuple3<Long, String, Integer> deserialize(byte[] bytes) throws IOException {
                String[] res = new String(bytes).split(",");
                Long timeStamp = Long.valueOf(res[0]);
                String dm = res[1];
                Integer value = Integer.valueOf(res[2]);
                return Tuple3.of(timeStamp, dm, value);
            }

            @Override
            public boolean isEndOfStream(Object o) {
                return false;
            }

            @Override
            public TypeInformation getProducedType() {
                return TypeInformation.of(new TypeHint<Tuple3<Long, String, Integer>>() {
                });
            }
        }, parameterTool.getProperties());

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<Tuple3<Long,String, Integer>> rateStream = env.addSource(rateConsumer);
        DataStream<Tuple5<Long, String, Integer, String, Integer>> orderStream = env.addSource(orderConsumer);

        long delay = 1000;

        DataStream<Tuple3<Long,String, Integer>> rateTimedStream = rateStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.milliseconds(delay)) {
            @Override
            public long extractTimestamp(Tuple3<Long, String, Integer> longStringIntegerTuple3) {
                return (Long) longStringIntegerTuple3.getField(0);
            }
        });

        DataStream<Tuple5<Long, String, Integer, String, Integer>> orderTimeedStream = orderStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<Long, String, Integer, String, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple5<Long, String, Integer, String, Integer> longStringIntegerStringIntegerTuple5) {
                return (Long)longStringIntegerStringIntegerTuple5.getField(0);
            }
        });

        DataStream<Tuple9<Long, String, Integer, String, Integer, Long, String, Integer, Integer>> joinedStream = orderTimeedStream.join(rateTimedStream).where(new KeySelector<Tuple5<Long, String, Integer, String, Integer>, String>() {
            @Override
            public String getKey(Tuple5<Long, String, Integer, String, Integer> longStringIntegerStringIntegerTuple5) throws Exception {
                return longStringIntegerStringIntegerTuple5.getField(3).toString();
            }
        }).equalTo(new KeySelector<Tuple3<Long, String, Integer>, String>() {
            @Override
            public String getKey(Tuple3<Long, String, Integer> longStringIntegerTuple3) throws Exception {
                return longStringIntegerTuple3.getField(1).toString();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Tuple5<Long, String, Integer, String, Integer>, Tuple3<Long, String, Integer>, Tuple9<Long, String, Integer, String, Integer, Long, String, Integer, Integer>>() {
                    @Override
                    public Tuple9<Long, String, Integer, String, Integer, Long, String, Integer, Integer> join(Tuple5<Long, String, Integer, String, Integer> first, Tuple3<Long, String, Integer> second) throws Exception {
                        Integer res = (Integer)first.getField(4) * (Integer)second.getField(2);
                        return Tuple9.of(first.f0, first.f1, first.f2, first.f3,first.f4, second.f0, second.f1, second.f2, res);
                    }
                });

        joinedStream.print();
        env.execute();
    }
}
