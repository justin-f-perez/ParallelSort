// Author: Justin Perez
// ABANDONED
// I wound up giving the sorter class its own main method.
; // This semi-colon is placed here intentionally to prevent compilation.
package hw1;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;


public class Main {
    // TODO: pick a reasonable default # of threads
    //   we should probably tune this based on observed performance, but
    //   I suspect we'll find that there are no performance gains for
    //   T > number of CPU cores
    public final static int defaultInputLength = (int) Math.pow(10, 7); // unit = # of long values
    public final static int defaultNumThreads = 10;
    public final static String defaultInputFileName = "array.bin";
    public final static String defaultOutputFileName = "sorted.bin"; // gets written to project root

    public static int numThreads;  // "T" in the HW1 description
    public static Path inputPath;  // "name of an input file containing an array of long integers to be sorted"
    public static Path outputPath; // "name of an output file that includes the sorted input array saved as an array of longs"
    public static long inputSize;  // in bytes

    public static void main(String[] args) throws IOException {
        // if no arguments are passed in and defaultInputFileName refers to a file that doesn't exist, then make one
        if (args.length < 1 && !new File(defaultInputFileName).exists()) createTestFile();

        long startTime = System.nanoTime();
        String inputFileName = (args.length < 1) ? defaultInputFileName : args[0];
        String outputFileName = (args.length < 2) ? defaultOutputFileName : args[1];
        numThreads = (args.length < 3) ? defaultNumThreads : Integer.parseInt(args[2]);
        inputPath = Paths.get(inputFileName);
        outputPath = Paths.get(outputFileName);

        // NOTE: our implementation uses memory mapped files which only supports
        // 32-bit addressing (i.e., max file size is 2 GB)
        // TODO: throw an exception if size > 2^32 bytes, or else implement a multi-buffer strategy
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

        // sort complete, stop the clock and do some verification
        long stopTime = System.nanoTime();
        double elapsedSeconds = (double) (stopTime - startTime) / Math.pow(10, 9);

        try (var fc = FileChannel.open(outputPath)) {
            var length = fc.size() / Long.BYTES;
            System.out.printf("Sorted %d elements in %,.2f seconds%n", length, elapsedSeconds);
        }

        verifySorted();
        checkSameElements();
    }

    private static void createTestFile() throws IOException {
        System.out.printf("Generating test input file %s with %d elements%n", defaultInputFileName, defaultInputLength);
        long generatorStart = System.nanoTime();
        new DataFileGenerator(defaultInputFileName, defaultInputLength).generate();
        long generatorStop = System.nanoTime();
        double generatorElapsedSeconds = (double) (generatorStop - generatorStart) / Math.pow(10, 9);
        System.out.printf("Finished writing test file in %,.3f seconds%n", generatorElapsedSeconds);
    }

    private static void verifySorted() throws IOException {
        System.out.println("Verifying output is sorted");
        try (var fc = FileChannel.open(outputPath)) {
            LongBuffer buffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size()).asLongBuffer();
            long previousValue = buffer.get();
            long currentValue;
            while (buffer.hasRemaining()) {
                currentValue = buffer.get();
                if (currentValue < previousValue) {
                    throw new RuntimeException("Output is not sorted!");
                }
                previousValue = currentValue;
            }
            System.out.println("Passed sort check");
        }
    }

    private static void checkSameElements() throws IOException {
        final int numChecks = 1000;
        System.out.println("Verifying that the input file and output file contain the same values");
        try (FileChannel unsortedFC = FileChannel.open(inputPath);
             FileChannel sortedFC = FileChannel.open(outputPath)) {
            if (unsortedFC.size() != sortedFC.size())
                throw new RuntimeException("Input/Output files are not the same size!");
            LongBuffer unsortedBuffer = unsortedFC.map(FileChannel.MapMode.READ_ONLY, 0, unsortedFC.size()).asLongBuffer();
            LongBuffer sortedBuffer = sortedFC.map(FileChannel.MapMode.READ_ONLY, 0, sortedFC.size()).asLongBuffer();
            long[] checkValues = new long[numChecks];
            unsortedBuffer.get(0, checkValues);

            int[] sortedCounts = countValues(sortedBuffer, checkValues);
            int[] unsortedCounts = countValues(unsortedBuffer, checkValues);
            if (!Arrays.equals(sortedCounts, unsortedCounts)) {
                throw new RuntimeException("Input file and output file do not contain the same values!");
            }
            for (int i = 0; i < checkValues.length; i++) {
                if (unsortedCounts[i] < 1 || sortedCounts[i] < 1) {
                    throw new RuntimeException("Something's not adding up here...");
                }
            }

        }
        System.out.println("Same-elements check passed.");
    }

    private static int[] countValues(LongBuffer buffer, long[] checkValues) {
        int[] valueCounts = new int[checkValues.length];
        while (buffer.hasRemaining()) {
            long current = buffer.get();
            for (int i = 0; i < checkValues.length; i++) {
                if (checkValues[i] == current) {
                    valueCounts[i] = valueCounts[i] + 1;
                }
            }
        }
        return valueCounts;
    }
}
