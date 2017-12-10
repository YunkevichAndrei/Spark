import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.distributed.*;

import org.junit.Test;

import service.MatrixService;

import java.io.File;


public class SparkTest {
    final SparkContext sc = new /* FileWorker.createRandomMatrix(3, 3);*/SparkContext(new SparkConf()
            .setAppName("Test").setMaster("local"));

    @Test
    public void countsWord() throws Exception {
        final File file = new File("data/input/file.txt");

        FileWorker.createRandomMatrix(100, 100);
        final BlockMatrix result = MatrixService.createBlockMatrixFromFile(sc, file.getAbsolutePath());

        FileWorker.outputMatrix(MatrixService.exp(result, 10));

    }


}
