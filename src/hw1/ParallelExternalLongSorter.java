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
import java.util.*;
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

class ParallelExternalLongSorter {
    static final int THREADPOOL_TIMEOUT_SECONDS = 60;
    //region constants
    // experimental findings: required overhead peaked at 70 MB w/ 4 threads on an 8 core machine
    //      having more or fewer threads reduced required overhead
    //      (required overhead=minimum to not throw an OOM heap error)
    private static final long MEMORY_OVERHEAD = 140 * MB;
    private static final Logger LOGGER = Logger.getLogger(ParallelExternalLongSorter.class.getName());
    private final Path inputPath;
    private final Path outputPath;
    private final Path tempFile;
    private final int nThreads;
    private long inputSize;
    //endregion

    public ParallelExternalLongSorter(Path inputPath, Path outputPath, int nThreads) throws IOException {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.nThreads = nThreads;
        LOGGER.info("either lying to you or verifying constructor args (enable assertions, add '-ea' in your JVM opts)");
        assert nThreads >= 1 : "must have at least 1 thread, not " + nThreads;
        assert !Files.isDirectory(inputPath) : "check yourself before you directoryour self";
        assert Files.isReadable(inputPath) : "input file is not readable";
        validateOutputPath(outputPath);
        this.tempFile =  Files.createTempFile("external-sort-scratch-space", ".tmp");
    }

    public void sort() throws IOException, InterruptedException, ExecutionException {
        LOGGER.info("starting setup");
        try (
                FileChannel inputFileChannel = FileChannel.open(inputPath, Set.of(READ));
                FileChannel scratchFileChannel = FileChannel.open(tempFile, Set.of(DELETE_ON_CLOSE, READ, WRITE));
                FileChannel outputFileChannel = FileChannel.open(outputPath, Set.of(CREATE, READ, WRITE))
        ) {

            this.inputSize = inputPath.toFile().length();

            if (inputSize == 0) {
                throw new RuntimeException("Abort: input file is empty");
            }
            //region make output files same size as input size
            LOGGER.info("JUST GIMME SOME ROOM TO BREATHE (preparing scratch space)");
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
            var maxMem = getMaximumTotalChunkMemory(MEMORY_OVERHEAD, 0, 0);
            int chunkCount = getChunkCount(nThreads, inputSize, maxMem);
            LOGGER.info("Count Chunkula: " + chunkCount);
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
                // TODO: what if instead of carving out chunks of a scratch file, create a private memory map?
                //      not sure if supported on Windows
                // TODO: check if OS is windows and install linux in the background
                scratchChunks[i] = scratchFileChannel.map(READ_ONLY, splits[i].bytePosition, splits[i].byteSize).asLongBuffer();
            }
            var outputBuffer = outputFileChannel.map(READ_WRITE, 0, inputSize).asLongBuffer();
            //endregion
            //region merge, unlock, make fun of java
            // NOTE: we always get at least 1 chunk because the floor for # of chunks is the # of threads
            var first = scratchChunks[0];
            // NOTE: we have at least 1 element, otherwise we would have thrown the Abort: empty input error
            LOGGER.info("(pre-merge) chunks[0][0]=%d chunks[0][-1]=%d".formatted(
                    first.get(0), first.get(first.limit() - 1)));
            LOGGER.info("(pre-merge) output[0][0]=%d output[0][-1]=%d".formatted(
                    outputBuffer.get(0), // NOTE: we have at least 1 element, otherwise we would have thrown an error earlier
                    outputBuffer.get(outputBuffer.limit() - 1)));
            merge(scratchChunks, outputBuffer);
            LOGGER.info("(post-merge) first element=" + outputBuffer.get(0) + " last=" + outputBuffer.get(outputBuffer.limit() - 1));
            outputLock.release();
            scratchLock.release();
            LOGGER.info("doing the world a favor and ending another java process (all done)");
            //endregion
        }
    }

    //region chunk count (how many splits/chunks should we make?)

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

        var chunkMerger = new ChunkMerger(presortedChunks, output);
        chunkMerger.merge();

        //region post-condition verification
        assert output.position() == output.limit() : "expected output buffer's position to be at limit"; // output is full
        assert Stream.of(presortedChunks).noneMatch(Buffer::hasRemaining) : "expected all chunks to be drained"; // all elements in scratch space written to output
        assert isSorted(output) : "expected output to be sorted";
        LOGGER.info("wow, the chunks are gone! you really can use coconut oil for everything (merge finished)");
        //endregion
    }
}
