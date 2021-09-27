// Author: Justin Perez

// TIP: cmd + . to (un)fold a //region

//region imports
package hw1;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static hw1.Utils.*;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;
//endregion

@SuppressWarnings("ClassCanBeRecord") // suppressed because colab notebook's version of java doesn't support 'record'
class ParallelExternalLongSorter {
    //region constants
    static final int THREADPOOL_TIMEOUT_SECONDS = 60;
    private static final int BASE_CHUNK_MULTIPLIER = 1;
    private final Path inputPath;
    private final Path outputPath;
    private final int nThreads;
    private static final Logger LOGGER = Logger.getLogger(ParallelExternalLongSorter.class.getName());
    //endregion

    public ParallelExternalLongSorter(Path inputPath, Path outputPath, int nThreads) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.nThreads = nThreads;
        //region pre-condition verification (and delete old output)
        LOGGER.info("either lying to you or verifying constructor args (enable assertions, add '-ea' in your JVM opts)");
        assert nThreads >= 1 : "must have at least 1 thread, not " + nThreads;
        assert !Files.isDirectory(inputPath) : "check yourself before you directoryour self";
        assert Files.isReadable(inputPath) : "input file is not readable";
        validateOutputPath(outputPath);
        //endregion
    }

    public void sort() throws IOException, InterruptedException, ExecutionException {
        LOGGER.info("starting setup");
        Path tempFile = Files.createTempFile("external-sort-scratch-space", ".tmp");
        try (
                FileChannel inputFileChannel = FileChannel.open(inputPath, Set.of(READ));
                FileChannel scratchFileChannel = FileChannel.open(tempFile, Set.of(DELETE_ON_CLOSE, READ, WRITE));
                FileChannel outputFileChannel = FileChannel.open(outputPath, Set.of(CREATE, READ, WRITE))
        ) {
            //region make output files same size as input size
            LOGGER.info("JUST GIMME SOME ROOM TO BREATHE (preparing scratch space)");
            long inputSize = inputFileChannel.size();
            scratchFileChannel.truncate(inputSize);
            outputFileChannel.truncate(inputSize);
            //endregion

            // run Arrays.parallelSort on whole file
            if (sortingMethod == "parallel" && nThreads > 1) {
                ChunkSorter chunkSorter = new ChunkSorter(inputFileChannel, outputFileChannel, new Split(0, inputFileChannel.size()/Long.BYTES), sortingMethod);
                chunkSorter.call();
                return;
            }

            //region plan where to split the input file
            LOGGER.info("can longs get covid? better put them in pods just to be safe (preparing chunks)");
            int chunkCount = getChunkCount(nThreads, inputSize);
            Split[] splits = Split.createSplits(inputSize, chunkCount);
            //endregion
            //region split it up (logically) by telling chunk sorters where they're going to sort
            ChunkSorter[] chunkSorters = new ChunkSorter[chunkCount];
            for (int i = 0; i < splits.length; i++) {
                var split = splits[i];
                var chunkSorter = new ChunkSorter(inputFileChannel, scratchFileChannel, split, sortingMethod);
                chunkSorters[i] = chunkSorter;
            }

            //endregion
            //region create a pool to manage threads, give it tasks (chunk sorter), then wait for it to finish executing them all
            LOGGER.info("these sheets are so soft! just look at that thread count: " + nThreads);
            LOGGER.info("is this expired? its getting chunky (queueing chunk sort jobs)");
            ExecutorService executor = Executors.newFixedThreadPool(nThreads);
            var futures = executor.invokeAll(List.of(chunkSorters));
            LOGGER.info("YOU'RE GONNA BE FRIENDS WHETHER YOU LIKE IT OR NOT! (joining threads)");
            executor.shutdown();  //tell the pool we're done giving it work
            LOGGER.info("GET TO THE CHOPPA! (awaiting threadpool termination)");
            // blocks until all tasks complete
            boolean timedOut = !executor.awaitTermination(THREADPOOL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (timedOut) throw new RuntimeException("threadpool timed out");
            for (var future : futures) future.get(); // propagate exceptions from child threads
            //endregion

            //region lock files, make scratch file chunks
            var scratchLock = scratchFileChannel.lock(0, scratchFileChannel.size(), false);
            var outputLock = outputFileChannel.lock(0, outputFileChannel.size(), false);
            var scratchChunks = new LongBuffer[(int) splits.length];
            for (int i = 0; i < splits.length; i++) {
                scratchChunks[i] = scratchFileChannel.map(READ_ONLY, splits[i].bytePosition, splits[i].byteSize).asLongBuffer();
            }
            var outputBuffer = outputFileChannel.map(READ_WRITE, 0, inputSize).asLongBuffer();
            //endregion
            //region merge, unlock, make fun of java
            LOGGER.info("first element (pre-merge)" + outputBuffer.get(0));
            LOGGER.info("last" + outputBuffer.get(outputBuffer.limit() - 1));
            merge(scratchChunks, outputBuffer);
            LOGGER.info("first element (post-merge)" + outputBuffer.get(0));
            LOGGER.info("last" + outputBuffer.get(outputBuffer.limit() - 1));
            outputLock.release();
            scratchLock.release();
            LOGGER.info("doing the world a favor and ending another java process (all done)");
            //endregion
        }
    }

    //region chunk count (how many splits/chunks should we make?)
    /*
    Returns the number of chunks that the input file should be split into.
    Considerations for the choice of count:
    (1) We have up to N `ChunkSorter`s each holding inputSize / chunkCount bytes in physical memory concurrently
    during the first pass
    (2) During the second pass, we should implement some data structure for efficiently
    taking the minimum value among the heads of the chunks (e.g., a min heap) - this also needs to be held in memory,
    but only after the first pass is complete
    (3) more chunks means we'll be doing more seeking within the file; the OS should be able to efficiently swap pages
    in and out of cache because we're using memory mapped IO and accessing each chunk sequentially, but for a
    ridiculously large number of chunks we won't have sufficient physical memory to hold the head of each chunk in
    cache concurrently. This would result in a lot of swapping between physical memory and disk
    (4) assuming the input is sufficiently large, chunk count should be >= parallelism
     */
    private int getChunkCount(int nThreads, long inputByteSize) {
        // TODO: is it always more efficient to initialize count to some multiple of parallelism?
        //      it could be the case that some chunks, by chance, happen to sort more quickly than others
        //      thus, it would be beneficial to have more (smaller) chunks so that all threads can stay busy
        //      until the entire input is sorted
        //      ... so if the answer is "yes", we should initialize chunkMultiplier to a higher value
        long workingMemoryLimit = getWorkingMemoryLimit(), expectedPeakMemoryUse;
        int count, chunkMultiplier = BASE_CHUNK_MULTIPLIER - 1;

        do {
            chunkMultiplier = chunkMultiplier + 1;
            count = nThreads * chunkMultiplier;
            expectedPeakMemoryUse = (inputByteSize / count) * nThreads;
        } while (expectedPeakMemoryUse > workingMemoryLimit && 60000 >= count && count >= 0);

        if (count > 60000) {
            // on most systems Java will choke if you try to do ~64k+ mmaps
            // src: https://mapdb.org/blog/mmap_files_alloc_and_jvm_crash/
            throw new RuntimeException("Aborting: Attempted to allocate too many chunks");
        }

        assert count > 0 : "integer overflow occurred while trying to get chunk count";
        LOGGER.info("Count Chunkula: " + count);
        var MB = Math.pow(10, 6);
        LOGGER.info("peak chonk (nThreads * MB / # of chunks): " + expectedPeakMemoryUse / MB);
        return count;
    }
    //endregion

    //region chunk sizing helper (how much room we have for in-memory sorting)
    private long getWorkingMemoryLimit() {
        // final long GB = (long) Math.pow(10, 9); // Gigabyte (SI unit)
        final long MB = (long) Math.pow(10, 6); // Megabyte (SI unit)

        // save some space on the heap for overhead
        // experimental findings: required overhead peaked at 70 MB w/ 4 threads on an 8 core machine
        //      having more or fewer threads reduced required overhead
        //      (required overhead=minimum to not throw an OOM heap error)
        final long overhead = 140 * MB;
        //  Maximum heap size: "Smaller of 1/4th of the physical memory or 1 GB"
        //      src: https://docs.oracle.com/javase/8/docs/technotes/guides/vm/gc-ergonomics.html
        var workingMemoryLimit = Runtime.getRuntime().maxMemory() - overhead;
        LOGGER.info("Pepperidge Farm remembers (MB): " + (workingMemoryLimit / MB));
        return workingMemoryLimit;
    }
    //endregion

    // TODO: too slow
    private void merge(LongBuffer[] presortedChunks, LongBuffer output) {
        //region precondition verification
        LOGGER.info("applying coconut oil (verifying merge preconditions)");
        // check preconditions
        assert output.position() == 0 : "expected output buffer to start at position 0";  // nothing has been written to output yet
        // output buffer is same size as cumulative size of presortedChunks
        assert output.capacity() == Stream.of(presortedChunks).mapToInt(Buffer::capacity).sum() : "expected output & scratch chunks to be same size";
        assert output.limit() == Stream.of(presortedChunks).mapToInt(Buffer::limit).sum() : "expected output & scratch chunks to be same size";
        // nothing has been read from presortedChunks yet
        assert Stream.of(presortedChunks).allMatch(c -> c.position() == 0) : "expected scratch chunks to be at position 0";
        // a little counter-intuitive, but they *should* all be 0  because 'position' is relative to the underlying byte
        // buffer's address
        assert IntStream.range(0, presortedChunks.length - 1).allMatch(
                i -> presortedChunks[i].limit() == 0 || presortedChunks[i].get(0) != presortedChunks[i + 1].get(0));
        for (var c : presortedChunks) assert isSorted(c) : "expected scratch chunks to be pre-sorted";
        //endregion

        //region implementation
        PriorityQueue<LongBuffer> minHeap = new PriorityQueue<>(new Utils.ChunkHeadComparator());
        // initially populate the heap with any non-empty chunk
        for (var chunk : presortedChunks) {
            if (chunk.hasRemaining()) minHeap.add(chunk);
        }
        LongBuffer chunkWithSmallestValue = minHeap.poll();
        while (chunkWithSmallestValue != null) {
            output.put(chunkWithSmallestValue.get()); // side effect: advances position fields of outputBuffer and chunk
            if (chunkWithSmallestValue.hasRemaining()) {
                minHeap.add(chunkWithSmallestValue);  // if this buffer still has elements, add it back into the heap
            }
            chunkWithSmallestValue = minHeap.poll();  // poll() returns null when the heap is empty
        }
        //endregion

        //region post-condition verification
        assert output.position() == output.limit() : "expected output buffer's position to be at limit"; // output is full
        assert Stream.of(presortedChunks).noneMatch(Buffer::hasRemaining) : "expected all chunks to be drained"; // all elements in scratch space written to output
        assert isSorted(output) : "expected output to be sorted";
        LOGGER.info("wow, the chunks are gone! you really can use coconut oil for everything (merge finished)");
        //endregion
    }
}
