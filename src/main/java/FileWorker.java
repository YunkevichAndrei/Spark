import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class FileWorker {
    public static void createRandomMatrix(int n, int m) {
        Random random = new Random();
        try (FileWriter writer = new FileWriter("data/input/file.txt", false)) {
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < m; j++) {
                    writer.append(i + "," + j + "," + random.nextInt(10) + "\n");
                }
            }
            writer.flush();
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void outputMatrix(final BlockMatrix matrix) {
        JavaRDD<MatrixEntry> etries = matrix.toCoordinateMatrix().entries().toJavaRDD();
        JavaRDD<String> output = etries.map(new Function<MatrixEntry, String>() {
            public String call(MatrixEntry e) {
                return String.format("%d,%d,%s", e.i(), e.j(), e.value());
            }
        });
        output.saveAsTextFile("data/output1");
    }
}
