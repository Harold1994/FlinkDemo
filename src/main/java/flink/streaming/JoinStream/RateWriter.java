package flink.streaming.JoinStream;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import javax.swing.plaf.synth.SynthEditorPaneUI;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 输出汇率到kafka
 * 时间戳（Long）
 * <p>
 * 货币类型（String）
 * <p>
 * 汇率（Integer）
 */
public class RateWriter {
    public static final String[] HBDM = {"BEF", "CNY", "DEM", "EUR", "HKD", "USD", "ITL"};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map props = new HashMap<String, String>();
        props.put("brokerList", "localhost:9092");
        props.put("topic", "rate");
        ParameterTool parameterTool = ParameterTool.fromMap(props);
        DataStream<String> messageStream = env.addSource(new SourceFunction<String>() {
            private Random r = new Random();
            private final long serialVersionUID = 1L;
            boolean running = true;

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (running) {
                    Thread.sleep(r.nextInt(3) * 1000);
                    sourceContext.collect(String.format("%d,%s,%d", System.currentTimeMillis(), HBDM[r.nextInt(HBDM.length)], r.nextInt(20)));
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        DataStreamSink<String> sink = messageStream.addSink(new FlinkKafkaProducer<String>(parameterTool.getRequired("brokerList"), parameterTool.getRequired("topic"), new SimpleStringSchema()));
        messageStream.print();
        env.execute();

    }
}
