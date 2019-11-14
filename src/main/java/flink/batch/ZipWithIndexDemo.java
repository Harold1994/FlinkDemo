package flink.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;

import java.util.logging.Logger;

public class ZipWithIndexDemo {
    public static void main(String[] args) throws Exception {
        Logger logger = Logger.getLogger(ZipWithIndexDemo.class.getSimpleName());
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        logger.info("create environment");
        env.setParallelism(2);
        DataSet<String> in = env.fromElements("A", "B", "C", "D", "E", "F", "G", "H", "A", "B", "C", "D", "E", "F", "G", "H");
        logger.info("begin");
        DataSet<Tuple2<Long, String>> res = DataSetUtils.zipWithUniqueId(in);
        res.writeAsCsv("./result.csv", "\n", ",");
        logger.info("done");
        env.execute("ZipWithIndexDemo");
    }
}
