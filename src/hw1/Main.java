package hw1;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ForkJoinPool;

public class Main {
    // TODO: pick a reasonable default # of threads
    //   we should probably tune this based on observed performance, but
    //   I suspect we'll find that there are no performance gains for
    //   T > number of CPU cores
    public final static int defaultNumThreads = 16;
    public final static String defaultInputFileName = "array.bin";
    public final static String defaultOutputFileName = "sorted.bin";
    public static int numThreads;  // "T" in the HW1 description
    public static Path inputPath;  // "name of an input file containing an array of long integers to be sorted"
    public static Path outputPath; // "name of an output file that includes the sorted input array saved as an array of longs"
    public static long inputSize;  // in bytes

    public static void main(String[] args) throws IOException {
        // parse args
        // defaults are provided for args that aren't passed in
        // if no arguments are passed in, a test data file is generated first
        if (args.length < 1) {
            new DataFileGenerator(defaultInputFileName, 100).generate();
        }
        String inputFileName = (args.length < 1) ? defaultInputFileName : args[0];
        String outputFileName = (args.length < 2) ? defaultOutputFileName : args[1];
        numThreads = (args.length < 3) ? defaultNumThreads : Integer.parseInt(args[2]);

        inputPath = Paths.get(inputFileName);
        outputPath = Paths.get(outputFileName);


        // NOTE: our implementation uses memory mapped files which only supports
        // 32-bit addressing (i.e., max file size is 2 GB)
        // TODO: throw an exception if size > 2 GB or implement a multi-buffer strategy
        inputSize = Files.size(inputPath);

        // now lets just do a straight copy from input to output so that we can
        // sort in-place on the output file
        Files.copy(inputPath, outputPath, StandardCopyOption.REPLACE_EXISTING);

        // get a read-write file
        try (RandomAccessFile raf = new RandomAccessFile(outputPath.toString(), "rw");
             // we can create memory maps from file channel
             FileChannel fc = raf.getChannel()) {

            // FileChannel fc = raf.getChannel();
            ForkSorter fs = new ForkSorter(fc, 0, inputSize, numThreads);
            ForkJoinPool pool = new ForkJoinPool();
            pool.invoke(fs);
        } // raf, fc auto-close here

        // check that the output actually got sorted
        var fc = FileChannel.open(outputPath);
        LongBuffer buff = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size()).asLongBuffer();
        long prev = buff.get();
        for (long current = buff.get(); buff.position() == buff.limit(); ) {
            if (prev > current) {
                throw new RuntimeException("Output is not sorted!");
            }
        }
        System.out.println("done");
    }
}
