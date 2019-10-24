package flink.streaming.Windowing;

import flink.streaming.WordCount.WordCount;
import flink.streaming.WordCount.WordCountData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowWordCount {
    public static void main(String[] args) throws Exception {
        ParameterTool param = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text;
        if (param.has("input")) {
            text = env.readTextFile(param.get("input"));
        } else {
            text = env.fromElements(WordCountData.WORDS);
        }

        env.getConfig().setGlobalJobParameters(param);

        final int windowSize = param.getInt("window", 10);
        final int slideSize = param.getInt("slide", 5);

        DataStream<Tuple2<String, Long>> count = text
                .flatMap(new WordCount.Tokenizer())
                .keyBy(0)
                .countWindow(windowSize, slideSize)
                .sum(1);

        if (param.has("output"))
            count.writeAsText(param.get("output"));
        else
            count.print();
        env.execute();
    }
}
