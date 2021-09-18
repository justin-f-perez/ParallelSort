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
 *     sequential_sort(input)
 *   else:
 *     parallel_merge_sort(first_half(input), T/2)
 *     parallel_merge_sort(last_half(input), T/2)
 *
 *
 * */

package hw1;

import java.io.IOException;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.RecursiveAction;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

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
        // parallel base case
        final int length = (int) (size / Long.BYTES);
        if (numThreads == 1 || length == 1) {
            sequentialSort(position, size);
        }
        // parallel recursive case
        else {
            final long halfSize = size / 2;  // TODO: what if size is odd?
            long mid = position + halfSize;
            invokeAll(
                    new ForkSorter(fc, position, halfSize, numThreads / 2),
                    new ForkSorter(fc, mid, halfSize, numThreads / 2)
            );
        }
    }

    // "sequential" meaning "single-threaded", as opposed to "parallel" meaning "multi-threaded"
    void sequentialSort(long position, long size) {
        final int length = (int) (size / Long.BYTES);
        if (length > 1) { // recursive case
            final long halfSize = size / 2;
            long mid = position + halfSize;
            sequentialSort(position, halfSize);
            sequentialSort(mid, halfSize);
            merge(position, size, mid);      // where all the actual sorting happens
        }
        // base case is just a no-op
    }

    void merge(long position, long size, long mid) {
        MappedByteBuffer mbb = null;
        try {
            mbb = fc.map(READ_WRITE, position, size);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        LongBuffer lb = mbb.asLongBuffer();
        long leftIndex = position;
        long rightIndex = mid;
        // TODO: implement merge
    }
}