package flink.streaming.Windowing;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class TopSpeedWindowing {
    public static void main(String[] args) throws Exception {
        final ParameterTool param = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setGlobalJobParameters(param);

        @SuppressWarnings({"rawtypes", "serial"})
        DataStream<Tuple4<Integer, Integer, Double, Long>> carData;
        if (param.has("input"))
            carData = env.readTextFile(param.get("input")).map(new ParseCarData());
        else {
            System.out.println("Executing TopSpeedWindowing example with default input data set.");
            System.out.println("Use --input to specify file input.");
            carData = env.addSource(CarSource.create(2));
        }

        int evictionSec = 10;
        double triggerMeters = 50;
        DataStream<Tuple4<Integer, Integer, Double, Long>> topSpeeds = carData
                .assignTimestampsAndWatermarks(new CarTimestamp())
                .keyBy(0)
                //The default window into which all data is placed
                .window(GlobalWindows.create())
                // 一个使元素保持一定时间的Evictor。 早于current_time  -  keep_time的元素被逐出。
                .evictor(TimeEvictor.of(Time.of(evictionSec, TimeUnit.SECONDS)))
                .trigger(DeltaTrigger.of(triggerMeters, new DeltaFunction<Tuple4<Integer, Integer, Double, Long>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public double getDelta(Tuple4<Integer, Integer, Double, Long> oldDataPoint, Tuple4<Integer, Integer, Double, Long> newDataPoint) {
                        return newDataPoint.f2 - oldDataPoint.f2;
                    }

                }, carData.getType().createSerializer(env.getConfig())))
                .maxBy(1);
        if (param.has("output"))
            topSpeeds.writeAsText(param.get("output"));
        else
            topSpeeds.print();
        env.execute();
    }


    private static class CarSource implements SourceFunction<Tuple4<Integer, Integer, Double, Long>> {
        private static final long serialVersionUID = 1L;
        private Integer[] speeds;
        private Double[] distance;

        private Random rand = new Random();

        private volatile boolean isRunning = true;

        private CarSource(int numOfCars) {
            speeds = new Integer[numOfCars];
            distance = new Double[numOfCars];
            Arrays.fill(speeds, 50);
            Arrays.fill(distance, 0d);
        }

        public static CarSource create(int cars) {
            return new CarSource(cars);
        }

        @Override
        public void run(SourceContext<Tuple4<Integer, Integer, Double, Long>> ctx) throws Exception {
            while (isRunning) {
                Thread.sleep(100);
                for (int carId = 0; carId < speeds.length; carId++) {
                    if (rand.nextBoolean()) {
                        speeds[carId] = Math.min(100, speeds[carId] + 5);
                    } else {
                        speeds[carId] = Math.max(0, speeds[carId] - 5);
                    }
                    distance[carId] += speeds[carId] / 3.6d;
                    Tuple4<Integer, Integer, Double, Long> record = new Tuple4<>(carId, speeds[carId], distance[carId], System.currentTimeMillis());
                    ctx.collect(record);
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static class ParseCarData extends RichMapFunction<String, Tuple4<Integer, Integer, Double, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple4<Integer, Integer, Double, Long> map(String s) throws Exception {
            String rawData = s.substring(1, s.length() - 1);
            String[] data = rawData.split(",");
            return new Tuple4<>(Integer.valueOf(data[0]), Integer.valueOf(data[1]), Double.valueOf(data[2]), Long.valueOf(data[3]));
        }
    }

    private static class CarTimestamp extends AscendingTimestampExtractor<Tuple4<Integer, Integer, Double, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(Tuple4<Integer, Integer, Double, Long> element) {
            return element.f3;
        }
    }
}
