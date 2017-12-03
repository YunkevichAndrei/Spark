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
        final File textFile = new File("data/input/text.txt");

        FileWorker.createRandomMatrix(3, 3);
        final BlockMatrix result = MatrixService.createBlockMatrixFromFile(sc, file.getAbsolutePath());
       /* final List<Tuple2<Integer, String>> map = WordCount.counts(sc(), words.getAbsolutePath());
        for (Tuple2<Integer, String> i : map)
            System.out.println(i.toString());
*/
        FileWorker.outputMatrix(result);
    }


}
