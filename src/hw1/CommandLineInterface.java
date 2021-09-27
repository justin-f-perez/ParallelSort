package hw1;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.logging.Logger;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.file.StandardOpenOption.*;

public class CommandLineInterface {
    static final String DEFAULT_INPUT_FILENAME = "array.bin";
    static final String DEFAULT_OUTPUT_FILENAME = "sorted.bin";
    static final int DEFAULT_NTHREADS = Runtime.getRuntime().availableProcessors();
    private static final Logger LOGGER = Logger.getLogger(CommandLineInterface.class.getName());

    public static void main(String[] args) throws Exception {
        //region arg parsing
        LOGGER.info(Arrays.toString(args));
        String inputFileName = (args.length < 1) ? DEFAULT_INPUT_FILENAME : args[0];
        String outputFileName = (args.length < 2) ? DEFAULT_OUTPUT_FILENAME : args[1];
        final int nThreads = (args.length < 3) ? DEFAULT_NTHREADS : Integer.parseInt(args[2]);
        final Path inputPath = Paths.get(inputFileName).toAbsolutePath();
        final Path outputPath = Paths.get(outputFileName).toAbsolutePath();
        LOGGER.info("you put your long ints in " + inputPath);
        LOGGER.info("you take your long ints out" + outputPath);
        LOGGER.info("hardcore, " + DEFAULT_NTHREADS + " cores");
        //endregion

        // cleanup from previous runs
        var oldOutputDeleted = outputPath.toFile().delete();
        LOGGER.info((oldOutputDeleted ? "get that nasty mess out of here " : "it was like that when i got here!") + outputPath);

        //region test data generation
        if (args.length >= 4) {
            final int inputLength = Integer.parseInt(args[3]); // unit = # of long values
            long generatorStart = System.nanoTime();
            new DataFileGenerator(inputPath.toString(), inputLength).generate();
            long generatorStop = System.nanoTime();
            double generatorElapsedSeconds = (double) (generatorStop - generatorStart) / Math.pow(10, 9);
            System.out.printf("Finished writing test file with %d long integers in %,.3f seconds%n",
                    inputLength, generatorElapsedSeconds);
        }
        //endregion

        var sorter = new ParallelExternalLongSorter(inputPath, outputPath, nThreads);
        sorter.sort();

        //region post-condition verification
        try (
                FileChannel inFC = FileChannel.open(inputPath, Set.of(READ));
                FileChannel outFC = FileChannel.open(outputPath, Set.of(READ))
        ) {
            assert (inFC.size() == outFC.size()) : "expected in size (" + inFC.size() + ") == out size (" + outFC.size() + ")";
            var in = inFC.map(READ_ONLY, 0, inFC.size()).asLongBuffer();
            var out = outFC.map(READ_ONLY, 0, outFC.size()).asLongBuffer();
            assert Utils.isEmpty(in) == Utils.isEmpty(out) : "expected output to be all 0's only if input is all 0's";
        }
        //endregion
    }
}
