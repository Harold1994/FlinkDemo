package flink.streaming.WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class WordCount {
    public static void main(String[] args) throws Exception {
        final ParameterTool para = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(para);

        DataStream<String> text;
        if (para.has("input")) {
            text = env.readTextFile(para.get("input"));
        } else {
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            text = env.fromElements(WordCountData.WORDS);
        }

        DataStream<Tuple2<String, Long>> counter = text
                .flatMap(new Tokenizer())
                .keyBy(0)
                .sum(1);

        counter.print();
        env.execute();
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Long>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
            String [] tokens = s.toLowerCase().split("\\W+");
            for (String token : tokens)
                collector.collect(new Tuple2<>(token,1L));
        }
    }
}
