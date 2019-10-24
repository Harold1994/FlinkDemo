package flink.batch.Utils;

import java.io.*;
import java.util.Random;

public class KMeansUtil {
    public static void main(String[] args) throws IOException {
//        File file = new File("points.csv");
//        if (!file.exists()) {
//            file.createNewFile();
//        }
//        FileWriter fw = new FileWriter(file.getAbsoluteFile());
//        BufferedWriter bw = new BufferedWriter(fw);
//
//        Random rand = new Random(10);
//        for (int i = 0; i<1000; i++) {
//            bw.write(rand.nextInt(1000) + " " + rand.nextInt(1000) + "\n");
//        }
//        bw.close();
//        fw.close();

        File file = new File("centroids.csv");
        if (!file.exists()) {
            file.createNewFile();
        }
        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);

        Random rand = new Random(10);
        for (int i = 0; i<10; i++) {
            bw.write(i + " " + rand.nextInt(1000) + " " + rand.nextInt(1000) + "\n");
        }
        bw.close();
        fw.close();

    }
}
