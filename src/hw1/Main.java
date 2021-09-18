/* The basic idea:
* given a number of threads T that we'll parallelize our sorting algorithm on,
* perform a merge sort where we spawn new threads each time we divide the input
* in half, where each thread has an exclusive memory-map of the region of the file
* it's responsible for sorting. Once the maximum number of threads is reached (T)
* then we stop spawning new threads and just continue with the merge sort within
* the current thread
*
*
*  caveats for below pseudocode:
*    assumes T is a power of 2
*    N is exactly the length of the input
*    assumes sequential_sort sorts in-place
*
* parallel_merge_sort (input, N, T):
*   if (N <= 1) OR T == 1:             # BASE CASE FOR PARALLEL RECURSION
*     sequential_sort(input[0...N])
*   else:
*     parallel_merge_sort(first_half[0...(N/2 - 1)], T/2)
*     parallel_merge_sort(last_half(input), T/2)
*
*
* */
package hw1;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

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


class ForkSorter extends RecursiveAction {

    private final int numThreads;
    private final long position;
    private final long size;
    FileChannel fc;

    ForkSorter(FileChannel fc, long position, long size, int numThreads) {
        this.fc = fc;
        this.position = position;
        this.size = size;
        this.numThreads = numThreads;
    }

    protected void compute() {
        // base case
        final int length = (int) (size / Long.BYTES);
        if (numThreads == 1 || length == 1) {
            try {
                sequentialSort();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        // recurse in parallel
        else {
            final long halfSize = size / 2;
            long mid = position + halfSize;
            invokeAll(
                    new ForkSorter(fc, position, halfSize, numThreads / 2),
                    new ForkSorter(fc, mid, halfSize, numThreads / 2)
            );
            try {
                merge();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    // "sequential" meaning "single-threaded", as opposed to "parallel" meaning "multi-threaded"
    void sequentialSort() throws IOException {
        MappedByteBuffer mbb = fc.map(READ_WRITE, position, size);
        LongBuffer lb = mbb.asLongBuffer();
        // TODO: sequentially sort the region of the file in `lb`
    }

    void merge() throws IOException {
        MappedByteBuffer mbb = fc.map(READ_WRITE, position, size);
        LongBuffer lb = mbb.asLongBuffer();
        // TODO: implement merge
    }
}
