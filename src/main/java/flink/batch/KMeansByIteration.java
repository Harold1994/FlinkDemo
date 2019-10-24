package flink.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.Collection;

public class KMeansByIteration {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        DataSet<Point> points = getPointDataSet(params, env);
        DataSet<Centroid> centroids = getCentroidDataSet(params, env);

        IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("iterations", 10));

        DataSet<Centroid> newCentroids = points.
                // 计算最近邻的中心点
                map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                // 计算并将每个中心点的坐标加起来
                .map(new CountAppender())
                .groupBy(0).reduce(new CentroidAccumulator())
                .map(new CentroidAverager());

        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);
        DataSet<Tuple2<Integer, Point>> clusteredPoints = points
                .map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

        // emit result
        if (params.has("output")) {
            clusteredPoints.writeAsCsv(params.get("output"), "\n", " ");

            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("KMeans Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            clusteredPoints.print();
        }
    }

    private static DataSet<Centroid> getCentroidDataSet(ParameterTool params, ExecutionEnvironment env) {
        DataSet<Centroid> centroids;
        if (params.has("centroids")) {
            centroids = env.readCsvFile(params.get("centroids"))
                    .fieldDelimiter(" ")
                    .pojoType(Centroid.class, "id", "x", "y");
        } else {
            throw new RuntimeException("must point out the file path of centroids, \n" +
                    "Use --centroids to specify file input.");
        }
        return centroids;
    }

    private static DataSet<Point> getPointDataSet(ParameterTool params, ExecutionEnvironment env) {
        DataSet<Point> points;
        if (params.has("points")) {
            points = env.readCsvFile(params.get("points"))
                    .fieldDelimiter(" ")
                    .pojoType(Point.class, "x", "y");
        } else {
            throw new RuntimeException("must point out the file path of points, \n" +
                    "Use --points to specify file input.");
        }
        return points;
    }

    public static class Point implements Serializable {
        public double x, y;

        public Point(){};

        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public Point add(Point other) {
            x += other.x;
            y += other.y;
            return this;
        }

        public Point div(long val) {
            x /= val;
            y /= val;
            return this;
        }

        public double euclideanDistance(Point other) {
            return Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
        }

        public void clean() {
            x = y = 0.0;
        }

        @Override
        public String toString() {
            return x + " " + y;
        }
    }

    public static class Centroid extends Point {
        public int id;

        public Centroid() {}

        public Centroid(int id, double x, double y) {
            super(x, y);
            this.id = id;
        }

        public Centroid(int id, Point point) {
            this(id, point.x, point.y);
        }

        @Override
        public String toString() {
            return id + " " + super.toString();
        }
    }

    private static class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {
        private Collection<Centroid> centroids;

        @Override
        public void open(Configuration parameters) throws Exception {
            centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Point> map(Point point) throws Exception {
            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            for (Centroid centroid : centroids) {
                double dis = point.euclideanDistance(centroid);
                if (dis < minDistance) {
                    minDistance  = dis;
                    closestCentroidId = centroid.id;
                }
            }

            return new Tuple2<>(closestCentroidId, point);
        }
    }

    private static class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {
        @Override
        public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> integerPointTuple2) throws Exception {
            return new Tuple3(integerPointTuple2.f0, integerPointTuple2.f1, 1l);
        }
    }

    private static class CentroidAccumulator implements org.apache.flink.api.common.functions.ReduceFunction<Tuple3<Integer, Point, Long>> {
        @Override
        public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> t0, Tuple3<Integer, Point, Long> t1) throws Exception {
            return new Tuple3<>(t0.f0, t0.f1.add(t1.f1), t0.f2 + t1.f2);
        }
    }

    private static class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Centroid>{
        @Override
        public Centroid map(Tuple3<Integer, Point, Long> val) throws Exception {
            return new Centroid(val.f0, val.f1.div(val.f2));
        }
    }
}

