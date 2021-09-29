package hw1;

import java.nio.LongBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class Utils {
    public static final int KB = (int) Math.pow(10, 3);
    public static final int MB = (int) Math.pow(10, 6);
    public static final int GB = (int) Math.pow(10, 9);
    // on most systems Java will choke if you try to do ~64k+ mmaps
    // src: https://mapdb.org/blog/mmap_files_alloc_and_jvm_crash/
    public static final int MAX_MAP_COUNT = 64000;
    public static final int LOG_BASE_2_OF_64000 = 16; // close enough
    // for a recursive algorithm that operates on 2 mmap input and 1 mmap output,
    // there may be up to n * log2(n) mmaps held simultaneously
    public static final int MAX_CHUNK_COUNT = MAX_MAP_COUNT / LOG_BASE_2_OF_64000; // 4000
    private static final Logger LOGGER = Logger.getLogger(Utils.class.getName());

    /**
     * This utility function gives an approximation for how much memory can safely be allocated
     * toward holding chunks of input data on the heap before an OOM error occurs.
     *
     * @param fixedOverhead        a fixed amount of memory to reserve, in bytes
     * @param dynamicOverhead      an amount of memory per concurrent chunk to reserve, in bytes
     * @param concurrentChunkCount number of chunks expected to be held in memory at the same time
     * @return the maximum amount of memory available, minus the calculated overhead
     */
    @SuppressWarnings("SameParameterValue")
    static long getMaximumTotalChunkMemory(long fixedOverhead, long dynamicOverhead, long concurrentChunkCount) {
        //  Maximum heap size: "Smaller of 1/4th of the physical memory or 1 GB"
        //      src: https://docs.oracle.com/javase/8/docs/technotes/guides/vm/gc-ergonomics.html
        //      this doesn't seem to have changed since then (as of september 2021)
        //      for oracle java, but other implementations may vary.
        long overhead = (dynamicOverhead * concurrentChunkCount) + fixedOverhead;
        return Runtime.getRuntime().maxMemory() - overhead;
    }

    static Class<? extends ChunkMerger> getChunkMergerClass(String className) throws ClassNotFoundException {
        return (Class<? extends ChunkMerger>) Class.forName(className);
    }


    /**
     * implementation is basically a binary search for the minimum returnValue satisfying the
     * conditions:
     * (1) nThreads * (inputSize / returnValue) <= availableMemory
     * (2) returnValue <= nThreads
     * (3) no other (known) system resource (e.g., max memory map count) would be exhausted if
     * (returnValue * log2 returnValue) of the resource is consumed
     *
     * @param nThreads        how many threads will concurrently hold a chunk in memory
     * @param inputSize       the size (bytes) of the input file to be split into chunks
     * @param availableMemory how much memory is available to each chunk
     * @return the minimum number of chunks that the inputByteSize must be split into
     */
    public static int getChunkCount(int nThreads, long inputSize, long availableMemory) {
        int lowerBound = nThreads;
        int upperBound = MAX_CHUNK_COUNT;
        if (lowerBound > upperBound)
            throw new RuntimeException("Cannot allocate enough memory maps to support this request");

        long memoryUsed;
        int count = lowerBound;

        while (lowerBound < upperBound) {
            count = (upperBound + lowerBound) / 2;
            memoryUsed = getMemoryUsed(nThreads, inputSize, count);

            if (memoryUsed > availableMemory) {
                // not enough mem, we need at least 1 more than current count
                count += 1;
                lowerBound = count;
            } else {
                // plenty of mem, we may (or may not) be able to use fewer chunks
                upperBound = count;
            }
            LOGGER.finest("nThreads=%d inputSize=%d count=%d availableMemory=%d memoryUsed=%d upperBound=%d lowerBound=%d".formatted(
                    nThreads, inputSize, count, availableMemory, memoryUsed, upperBound, lowerBound));
        }
        if (getMemoryUsed(nThreads, inputSize, count) > availableMemory) {
            LOGGER.severe("nThreads=%d inputSize=%d count=%d availableMemory=%d".formatted(
                    nThreads, inputSize, count, availableMemory));
            throw new RuntimeException(
                    "It's not possible to sort an input of size " + inputSize + "with only " + availableMemory);
        }
        return count;
    }

    private static long getMemoryUsed(int nThreads, long inputSize, int count) {
        long chunkSize = (inputSize / count);
        return chunkSize * nThreads;
    }

    /**
     * Only use for test/validation (uses {@link Stream#parallel}, which is probably not allowed
     * for the core implementation of this assignment.)
     *
     * @param chunk chunk to check for sortedness
     * @return true if the values are sorted in ascending order, else false
     */
    public static boolean isSorted(LongBuffer chunk) {
        return IntStream.range(0, chunk.limit() - 1).parallel().allMatch(i -> chunk.get(i) <= chunk.get(i + 1));
    }

    /**
     * Only use for test/validation (uses {@link Stream#parallel}, which is probably not allowed
     * for the core implementation of this assignment.)
     *
     * @param chunks chunks to check for sortedness
     * @return true if each chunk is locally sorted, else false
     */
    public static boolean allSorted(LongBuffer[] chunks) {
        return Arrays.stream(chunks).parallel().allMatch(Utils::isSorted);
    }

    /**
     * @param chunk chunk to check for emptiness
     * @return true if all values in the chunk are empty, else false
     */
    public static boolean isEmpty(LongBuffer chunk) {
        return IntStream.range(0, chunk.limit()).allMatch(i -> chunk.get(i) == 0);
    }

    public static void validateOutputPath(Path outputPath) {
        // cuz if the output file already exists and its already sorted, we can't tell if the program worked or
        // if it just happened to not crash
        assert !outputPath.toFile().exists() : "expected outputPath not to exist";
        Path parent = outputPath.getParent();
        assert Files.exists(parent) : "parent directory of output path doesn't exist";
        assert Files.isWritable(parent) : "parent directory of output path isn't writable";
    }

    public static class ChunkHeadComparator implements Comparator<LongBuffer> {
        /**
         * @param x a chunk to compare
         * @param y another chunk to compare
         * @return see: {@link Long#compare(long, long) Long.compare}
         */
        @Override
        public int compare(LongBuffer x, LongBuffer y) {
            return Long.compare(x.get(x.position()), y.get(y.position()));
        }
    }
}
