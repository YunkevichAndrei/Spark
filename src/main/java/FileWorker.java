import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * The service is for work with files.
 *
 * @author Andrei Yunkevich
 */
public class FileWorker {

    /**
     * Create and record a random matrix in the file.
     *
     * @param row matrix rows
     * @param col matrix columns
     */
    public static void createRandomMatrix(int row, int col) {
        Random random = new Random();
        try (FileWriter writer = new FileWriter("data/input/file.txt", false)) {
            for (int i = 0; i < row; i++) {
                for (int j = 0; j < col; j++) {
                    writer.append(i + "," + j + "," + random.nextInt(10) + "\n");
                }
            }
            writer.flush();
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    /**
     * Record BlockMatrix in the file.
     *
     * @param matrix the BlockMatrix
     */
    public static void outputMatrix(final BlockMatrix matrix) {
        JavaRDD<MatrixEntry> entries = matrix.toCoordinateMatrix().entries().toJavaRDD();
        JavaRDD<String> output = entries.map((Function<MatrixEntry, String>)
                e -> String.format("%d,%d,%s", e.i(), e.j(), e.value()));
        output.saveAsTextFile("data/output1");
    }
}
