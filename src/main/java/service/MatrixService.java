package service;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;

import java.util.ArrayList;
import java.util.List;

/**
 * The service is for work with matrix.
 *
 * @author Andrei Yunkevich
 */
public class MatrixService {

    /**
     * Convert JavaRDD<String> to JavaRDD<MatrixEntry>.
     *
     * @param rdd the string rdd
     * @return matrixEntry rdd
     */
    public static JavaRDD<MatrixEntry> rddMatrixEntry(JavaRDD<String> rdd) {
        JavaRDD<MatrixEntry> entries = rdd.map((Function<String, MatrixEntry>) x -> {
            String[] entryValues = x.split(",");
            long i = Long.parseLong(entryValues[0]);
            long j = Long.parseLong(entryValues[1]);
            double value = Double.parseDouble(entryValues[2]);
            return new MatrixEntry(i, j, value);
        });
        return entries;
    }

    /**
     * Create BlockMatrix from file.
     *
     * @param sc   the Spark Context
     * @param file the file with matrix
     * @return BlockMatrix
     */
    public static BlockMatrix createBlockMatrixFromFile(final SparkContext sc, final String file) {
        JavaRDD<String> rdd = sc.textFile(file, 1).toJavaRDD();
        JavaRDD<MatrixEntry> entries = rddMatrixEntry(rdd);
        CoordinateMatrix coordinateMatrix = new CoordinateMatrix(entries.rdd());
        BlockMatrix blockMatrix = coordinateMatrix.toBlockMatrix().cache();
        blockMatrix.validate();
        return blockMatrix;
    }

    /**
     * Calculating the factorial of a number.
     *
     * @param n the number
     * @return factorial of a number
     */
    public static long factorial(long n) {
        return n <= 1 ? 1 : n * factorial(n - 1);
    }

    /**
     * Matrix multiplication by number.
     *
     * @param matrix the BlockMatrix
     * @param number the number
     * @return BlockMatrix
     */
    public static BlockMatrix replaceMatrix(final BlockMatrix matrix, double number) {
        JavaRDD<MatrixEntry> entryJavaRDD = matrix.toCoordinateMatrix().entries().toJavaRDD();
        JavaRDD<String> output = entryJavaRDD.map((Function<MatrixEntry, String>) e -> String
                .format("%d,%d,%s", e.i(), e.j(), e.value() * number));
        JavaRDD<MatrixEntry> entries = rddMatrixEntry(output);
        CoordinateMatrix coordinateMatrix = new CoordinateMatrix(entries.rdd());
        BlockMatrix blockMatrix = coordinateMatrix.toBlockMatrix().cache();
        blockMatrix.validate();
        return blockMatrix;
    }

    /**
     * Adding an identity matrix.
     *
     * @param matrix the BlockMatrix
     * @return new BlockMatrix
     */
    public static BlockMatrix addEye(final BlockMatrix matrix) {
        JavaRDD<MatrixEntry> entriesJavaRDD = matrix.toCoordinateMatrix().entries().toJavaRDD();
        JavaRDD<String> output = entriesJavaRDD.map((Function<MatrixEntry, String>) e -> {
            if (e.i() == e.j())
                return String.format("%d,%d,%s", e.i(), e.j(), e.value() + 1.0);
            return String.format("%d,%d,%s", e.i(), e.j(), e.value());
        });
        JavaRDD<MatrixEntry> entries = rddMatrixEntry(output);
        CoordinateMatrix coordinateMatrix = new CoordinateMatrix(entries.rdd());
        BlockMatrix blockMatrix = coordinateMatrix.toBlockMatrix().cache();
        blockMatrix.validate();
        return blockMatrix;
    }

    /**
     * Power of matrix.
     *
     * @param matrix the BlockMatrix
     * @param n      power
     * @return new BlockMatrix
     */
    public static List<BlockMatrix> pow(BlockMatrix matrix, int n) {
        final List<BlockMatrix> blockMatrices = new ArrayList<>();
        blockMatrices.add(matrix);

        for (int i = 0; i < n - 1; i++)
            blockMatrices.add(blockMatrices.get(blockMatrices.size() - 1).multiply(matrix));

        return blockMatrices;
    }

    /**
     * Exponent of matrix.
     *
     * @param matrix the BlockMatrix
     * @param step   number of addendum
     * @return new BlockMatrix
     */
    public static BlockMatrix exp(final BlockMatrix matrix, int step) {
        final List<BlockMatrix> powers = pow(matrix, step - 1);
        BlockMatrix blockMatrix = matrix;

        for (int i = 1; i < step; i++) {
            blockMatrix = blockMatrix.add(replaceMatrix(powers.get(i - 1), (1.0 / factorial(i))));
            //System.out.println(blockMatrix.toLocalMatrix().toString());
        }
        blockMatrix = addEye(blockMatrix);

        return blockMatrix;
    }

}
