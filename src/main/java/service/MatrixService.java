package service;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.distributed.BlockMatrix;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;

import java.util.ArrayList;
import java.util.List;

public class MatrixService {
    public static JavaRDD<MatrixEntry> rddMatrixEntry(JavaRDD<String> rdd) {
        JavaRDD<MatrixEntry> entries = rdd.map((Function<String, MatrixEntry>) x -> {
            String[] indeceValue = x.split(",");
            long i = Long.parseLong(indeceValue[0]);
            long j = Long.parseLong(indeceValue[1]);
            double value = Double.parseDouble(indeceValue[2]);
            return new MatrixEntry(i, j, value);
        });
        return entries;
    }

    public static BlockMatrix createBlockMatrixFromFile(final SparkContext sc, final String file) {
        JavaRDD<String> rdd = sc.textFile(file, 1).toJavaRDD();
        JavaRDD<MatrixEntry> entries = rddMatrixEntry(rdd);
        // Create a CoordinateMatrix from a JavaRDD<MatrixEntry>.
        CoordinateMatrix coordinateMatrix = new CoordinateMatrix(entries.rdd());
        // Transform the CoordinateMatrix to a BlockMatrix
        BlockMatrix blockMatrix = coordinateMatrix.toBlockMatrix().cache();

        // Validate whether the BlockMatrix is set up properly. Throws an Exception when it is not valid.
        // Nothing happens if it is valid.
        blockMatrix.validate();
        return blockMatrix;
    }

    public static long factorial(long n) {
        return n <= 1 ? 1 : n * factorial(n - 1);
    }

    public static BlockMatrix replaceMatrix(final BlockMatrix matrix, double number) {
        JavaRDD<MatrixEntry> etries = matrix.toCoordinateMatrix().entries().toJavaRDD();
        JavaRDD<String> output = etries.map((Function<MatrixEntry, String>) e -> String
                .format("%d,%d,%s", e.i(), e.j(), e.value() * number));
        JavaRDD<MatrixEntry> entriess = rddMatrixEntry(output);
        // Create a CoordinateMatrix from a JavaRDD<MatrixEntry>.
        CoordinateMatrix coordinateMatrix = new CoordinateMatrix(entriess.rdd());
        // Transform the CoordinateMatrix to a BlockMatrix
        BlockMatrix blockMatrix = coordinateMatrix.toBlockMatrix().cache();

        // Validate whether the BlockMatrix is set up properly. Throws an Exception when it is not valid.
        // Nothing happens if it is valid.
        blockMatrix.validate();
        return blockMatrix;
    }

    public static BlockMatrix addEye(final BlockMatrix matrix) {
        JavaRDD<MatrixEntry> etries = matrix.toCoordinateMatrix().entries().toJavaRDD();
        JavaRDD<String> output = etries.map(new Function<MatrixEntry, String>() {
            @Override
            public String call(MatrixEntry e) throws Exception {
                if (e.i() == e.j())
                    return String
                            .format("%d,%d,%s", e.i(), e.j(), e.value() + 1.0);
                return String
                        .format("%d,%d,%s", e.i(), e.j(), e.value());
            }
        });
        JavaRDD<MatrixEntry> entriess = rddMatrixEntry(output);
        // Create a CoordinateMatrix from a JavaRDD<MatrixEntry>.
        CoordinateMatrix coordinateMatrix = new CoordinateMatrix(entriess.rdd());
        // Transform the CoordinateMatrix to a BlockMatrix
        BlockMatrix blockMatrix = coordinateMatrix.toBlockMatrix().cache();

        // Validate whether the BlockMatrix is set up properly. Throws an Exception when it is not valid.
        // Nothing happens if it is valid.
        blockMatrix.validate();
        return blockMatrix;
    }

    public static List<BlockMatrix> pow(BlockMatrix matrix, int n) {
        final List<BlockMatrix> blockMatrices = new ArrayList<>();
        blockMatrices.add(matrix);

        for (int i = 0; i < n - 1; i++)
            blockMatrices.add(blockMatrices.get(blockMatrices.size() - 1).multiply(matrix));

        return blockMatrices;
    }

    public static BlockMatrix exp(BlockMatrix matrix, int step) {
        List<BlockMatrix> powers = pow(matrix, step - 1);
        BlockMatrix blockMatrix = matrix;

        for (int i = 1; i < step; i++) {
            blockMatrix = blockMatrix.add(replaceMatrix(powers.get(i - 1), (1.0 / factorial(i))));
            //System.out.println(blockMatrix.toLocalMatrix().toString());
        }

        return blockMatrix;
    }

}
