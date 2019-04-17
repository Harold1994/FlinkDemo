package flink.streaming.twitter;

import flink.streaming.twitter.util.TwitterExampleData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.StringTokenizer;

/**
 * 计算json流中使用最多的词
 * The input is a Tweet stream from a TwitterSource.
 */
public class TwitterExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> source = env.fromElements(TwitterExampleData.TEXTS);
        SingleOutputStreamOperator<Tuple2<String, Integer>> tweets = source
                .flatMap(new SelectEnglishAndTokenizeFlatMap())
                .keyBy(0)
                .sum(1);
        tweets.print();
        env.execute();
    }

    public static class SelectEnglishAndTokenizeFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        private transient ObjectMapper jsonParser;

        /**
         * Select the language from the incoming JSON text.
         */
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang") && jsonNode.get("user").get("lang").asText().equals("en");
            boolean hasText = jsonNode.has("text");
            if (isEnglish && hasText) {
                // message of tweet
                StringTokenizer tokenizer = new StringTokenizer(jsonNode.get("text").asText());

                // split the message
                while (tokenizer.hasMoreTokens()) {
                    String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();

                    if (!result.equals("")) {
                        out.collect(new Tuple2<>(result, 1));
                    }
                }
            }
        }
    }
}
