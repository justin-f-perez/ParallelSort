/*   ABANDONED
 * This was my first attempt at solving the problem. However, I started from scratch
 * with a FixedThreadPool because it's not clear to me that the 'parallelism' parameter
 * actually sets a hard limit on the number of concurrently executing threads. - Justin
 *
 * Author: Justin Perez
 *
 * The basic idea:
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
; // This semi-colon is placed here intentionally to prevent compilation.
package hw1;

import java.io.IOException;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Spliterators;
import java.util.concurrent.RecursiveAction;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class ForkSorter extends RecursiveAction {

    private final int numThreads;
    private final long position;
    private final long size;
    private final int length;
    private final LongAlignedHalves halves;
    FileChannel fc;

    ForkSorter(FileChannel fc, long position, long size, int numThreads) {
        this.fc = fc;
        this.position = position;
        this.size = size;
        this.numThreads = numThreads;
        this.length = (int) (size / Long.BYTES);
        this.halves = new LongAlignedHalves(position, size);
    }

    protected void compute() {
        try {
            if (numThreads > 1 && length > 1) { // parallel recursive case
                // DEBUG
//                System.out.println(
//                    "\nwhole position " + String.valueOf(this.position) +
//                    "\nwhole size " + String.valueOf(this.size) +
//                    this.halves);

                invokeAll(
                        new ForkSorter(fc, this.halves.leftPosition, this.halves.leftSize, numThreads / 2),
                        new ForkSorter(fc, this.halves.rightPosition, this.halves.rightSize, numThreads / 2)
                );
            }
            sort(fc, position, size);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void externalHeapSort(FileChannel fc, long position, long size) throws IOException {
        var lock = fc.lock(position, size, false);
        lock.release();
    }

    private void sort(FileChannel fc, long position, long size) throws IOException {
        var lock = fc.lock(position, size, false);
        var mbb = fc.map(READ_WRITE, position, size);
        LongBuffer buf = mbb.asLongBuffer();

        // just for testing
        // this does crash for large inputs as we eventually read the entire file into memory...
        // tested by setting -xmx256m and attempting to sort a 763M file
        long[] arr = new long[buf.limit()];
        buf.get(arr);
        Arrays.sort(arr);
        buf.flip();
        buf.put(arr);

        lock.release();

    }

    // swaps lb[left] and lb[right] if lb[right] < lb[left]
    // return value = whether a swap happened
    boolean swap(LongBuffer lb, int leftIndex, int rightIndex) {
        var leftVal = lb.get(leftIndex);
        var rightVal = lb.get(rightIndex);
        if (rightVal < leftVal) {
            lb.put(leftIndex, rightVal);
            lb.put(rightIndex, leftVal);
            return true;
        } else {
            return false;
        }
    }

    class LongAlignedHalves {
        // what if size/2 splits a long in half?
        //  e.g., if we have 3 longs initially then we'll end up
        //    with 1.5 in each half
        // therefore, we must align the halves to long boundaries
        public long leftPosition;
        public long rightPosition;
        public long leftSize;
        public long rightSize;

        public LongAlignedHalves(long position, long size) {
            this.leftPosition = position;
            final long rawHalfSize = size / 2;
            long partialSize = rawHalfSize % Long.BYTES;
            boolean isPartial = partialSize > 0;
            this.leftSize = rawHalfSize - partialSize; // drop partial long
            this.rightSize = leftSize + (isPartial ? Long.BYTES : 0);
            this.rightPosition = position + leftSize;
        }

        public String toString() {
            return "\nleft position" + this.leftPosition +
                    "\nleft size" + this.leftSize +
                    "\nright position" + this.rightPosition +
                    "\nright size" + this.rightSize;
        }
    }

}