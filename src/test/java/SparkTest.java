import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.mllib.linalg.distributed.*;

import org.junit.Test;

import service.MatrixService;

import java.io.File;


public class SparkTest {
    final SparkContext sc = new SparkContext(new SparkConf()
            .setAppName("Test").setMaster("local"));

    @Test
    public void countsWord() throws Exception {
        final File file = new File("data/input/file.txt");

        FileWorker.createRandomMatrix(100, 100);
        final BlockMatrix result = MatrixService.createBlockMatrixFromFile(sc, file.getAbsolutePath());

        FileWorker.outputMatrix(MatrixService.exp(result, 10));
    }

}
