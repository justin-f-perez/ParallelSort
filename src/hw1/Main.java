package hw1;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ForkJoinPool;

public class Main {

    public static int numThreads;  // "T" in the HW1 description
    public static Path inputPath;  // "name of an input file containing an array of long integers to be sorted"
    public static Path outputPath; // "name of an output file that includes the sorted input array saved as an array of longs"
    public static long inputSize;  // in bytes

    public static void main(String[] args) throws IOException {

        // TODO: pick a reasonable default; we should probably tune this based on
        //   observed performance, but I suspect we'll find that there are no performance
        //   gains for T > number of CPU cores
        final int defaultNumThreads = 1;
        inputPath = Paths.get(args[0]);
        outputPath = Paths.get(args[1]);
        numThreads = (args.length < 3) ? defaultNumThreads : Integer.parseInt(args[2]);

        // NOTE: our implementation uses memory mapped files which only supports
        // 32-bit addressing (i.e., max file size is 2 GB)
        // TODO: throw an exception if size > 2 GB or implement a multi-buffer strategy
        inputSize = Files.size(inputPath);

        // now lets just do a straight copy from input to output so that we can
        // sort in-place on the output file
        Files.copy(inputPath, outputPath, StandardCopyOption.REPLACE_EXISTING);

        // do all the boilerplate garbage to get something (fc) we can create memory maps from
        try (RandomAccessFile raf = new RandomAccessFile(outputPath.toString(), "rw")) {
            FileChannel fc = raf.getChannel();
            ForkSorter fs = new ForkSorter(fc, 0, inputSize, numThreads);
            ForkJoinPool pool = new ForkJoinPool();
            pool.invoke(fs);
        } // I believe `raf` is auto-closed here, which I assume flushes any buffered writes
    }
}
