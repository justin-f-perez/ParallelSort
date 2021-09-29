// Author: Justin Perez

// TIP: cmd + . to (un)fold a //region

//region imports
package hw1;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.Buffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
    private final long inputSize;
    private final long maxMem;
    private final int chunkCount;
    private final Class<? extends ChunkMerger> chunkMergerType;
    private ArrayList<Split> remainingSplits;
    //endregion

    public ParallelExternalLongSorter(Path inputPath, Path outputPath, int nThreads, Class<? extends ChunkMerger> chunkMergerType) throws IOException {
        this.chunkMergerType = chunkMergerType;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.nThreads = nThreads;
        LOGGER.info("either lying to you or verifying constructor args (enable assertions, add '-ea' in your JVM opts)");
        assert nThreads >= 1 : "must have at least 1 thread, not " + nThreads;
        assert !Files.isDirectory(inputPath) : "check yourself before you directoryour self";
        assert Files.isReadable(inputPath) : "input file is not readable";
        validateOutputPath(outputPath);
        this.tempFile = Files.createTempFile("external-sort-scratch-space", ".tmp");
        this.inputSize = inputPath.toFile().length();
        if (inputSize == 0) throw new RuntimeException("Abort: input file is empty");
        this.maxMem = getMaximumTotalChunkMemory(MEMORY_OVERHEAD, 0, 0);
        this.chunkCount = getChunkCount(this.nThreads, inputSize, maxMem);
        var splits = Split.createSplits(inputSize, chunkCount);
        this.remainingSplits = new ArrayList<>(List.of(splits));
    }

    public void sort() throws IOException, InterruptedException, ExecutionException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        LOGGER.info("starting setup");
        try (
                FileChannel inputFileChannel = FileChannel.open(inputPath, Set.of(READ));
                FileChannel scratchFileChannel = FileChannel.open(tempFile, Set.of(READ, WRITE))
        ) {
            if (inputSize != inputFileChannel.size()) throw new RuntimeException("Abort: input file changed on disk");
            LOGGER.info("JUST GIMME SOME ROOM TO BREATHE (preparing scratch space)");

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
                var chunkSorter = new ChunkSorter(inputFileChannel, scratchFileChannel, split);
                chunkSorters[i] = chunkSorter;
            }
            //endregion
            //region create a pool to manage threads, give it tasks (chunk sorter), then wait for it to finish executing them all
            LOGGER.info("these sheets are so soft! just look at that thread count: " + nThreads);
            LOGGER.info("is this expired? its getting chunky (queueing chunk sort jobs)");
            ExecutorService executor = Executors.newFixedThreadPool(nThreads);
            var futures = executor.invokeAll(List.of(chunkSorters));
            LOGGER.info("YUCK! SOMEONE SORTED IN THE POOL! (joining sort threads)");
            executor.shutdown();  //tell the pool we're done giving it work
            LOGGER.info("I'LL BE BACK! (awaiting sort threadpool termination)");
            // blocks until all tasks complete
            boolean timedOut = !executor.awaitTermination(THREADPOOL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (timedOut) throw new RuntimeException("sort threadpool timed out");
            LOGGER.info("Back to the future- now with parallel timelines! (propagating sorting exceptions)");
            for (var future : futures) future.get(); // propagate exceptions from child threads
            LOGGER.info("Nevermind, the timelines collapsed. (no exceptions)");
            //endregion
        }
        try (
                FileChannel scratchFileChannel = FileChannel.open(tempFile, Set.of(DELETE_ON_CLOSE, READ, WRITE));
                FileChannel outputFileChannel = FileChannel.open(outputPath, Set.of(CREATE, READ, WRITE))
        ) {
            //region merge & make fun of java

            //  we're going to merge back and forth between scratch and output, and fix it up at the end
            // i.e., 1st iteration channels[dst] == scratch, then we'll do dst = (dst + 1) % 2
            // then at the end if the last channels[dst] we wrote to was scratch, we'll just do a quick swap
            int src = 0, dst = 1;
            var channels = new FileChannel[]{scratchFileChannel, outputFileChannel};
            while (remainingSplits.size() > 1) {
                var mergers = new ArrayList<ChunkMerger>();
                ArrayList<Split> mergedSplits = new ArrayList<>();
                var splitPairs = Split.groupSplits(remainingSplits).iterator();
                LongBuffer[] srcChunks = null;
                LongBuffer dstBuffer = null;

                assert splitPairs.hasNext();
                while (splitPairs.hasNext()) {
                    var pair = splitPairs.next();
                    assert pair.length == 2 || pair.length <= 1;

                    Split mergedSplit = (pair.length == 2) ? new Split(pair[0], pair[1]) : pair[0];
                    mergedSplits.add(mergedSplit);

                    srcChunks = getChunks(channels[src], pair);
                    dstBuffer = channels[dst].map(READ_WRITE, mergedSplit.bytePosition, mergedSplit.byteSize).asLongBuffer();
                    mergers.add(makeChunkMerger(srcChunks, dstBuffer));
                }
                //region merge pool
                checkMergePreconditions(srcChunks, dstBuffer);
                ExecutorService executor = Executors.newFixedThreadPool(nThreads);
                var futures = executor.invokeAll(mergers);
                LOGGER.info("YOU'RE GONNA BE FRIENDS WHETHER YOU LIKE IT OR NOT! (joining merge threads)");
                executor.shutdown();  //tell the pool we're done giving it work
                LOGGER.info("GET TO THE CHOPPA! (awaiting merge threadpool termination)");
                boolean timedOut1 = !executor.awaitTermination(THREADPOOL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (timedOut1) throw new RuntimeException("merge threadpool timed out");
                LOGGER.info("Hunting bugs from the future- im-doing-my-part.gif (propagating sorting exceptions)");
                for (var future : futures) future.get(); // propagate exceptions from child threads
                LOGGER.info("The only good bug is a dead bug. (no exceptions)");
                //endregion
                checkMergePostconditions(srcChunks, dstBuffer);
                LOGGER.info("Ti esrever dna ti pilf nwod gnaht ym tup i");
                // swap src/dst
                src = (src + 1) % 2;
                dst = (dst + 1) % 2;
                this.remainingSplits = mergedSplits;
            }
            // src and dst are reversed from the tail of the last iteration of the loop
            boolean shouldSwap = channels[dst].equals(outputFileChannel);
            if (shouldSwap) scratchFileChannel.transferTo(0, scratchFileChannel.size(), outputFileChannel);
            LOGGER.info("doing the world a favor and ending another java process (all done)");
        }
    }

    private ChunkMerger makeChunkMerger(LongBuffer[] srcChunks, LongBuffer dstBuffer) throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        return this.chunkMergerType.getDeclaredConstructor(
                LongBuffer[].class, LongBuffer.class).newInstance(srcChunks, dstBuffer);
    }

    private LongBuffer[] getChunks(FileChannel scratchFileChannel, Split[] splits) throws IOException {
        // TODO: what if instead of carving out chunks of a scratch file, create a private memory map?
        //      not sure if supported on Windows
        // TODO: check if OS is windows and install linux in the background
        var scratchChunks = new LongBuffer[(int) splits.length];
        for (int i = 0; i < splits.length; i++) {
            scratchChunks[i] = scratchFileChannel.map(READ_ONLY, splits[i].bytePosition, splits[i].byteSize).asLongBuffer();
        }
        return scratchChunks;
    }

    private void checkMergePostconditions(LongBuffer[] presortedChunks, LongBuffer output) {
        //region post-condition verification
        LOGGER.info("(post-merge) first element=" + output.get(0) + " last=" + output.get(output.limit() - 1));
        assert output.position() == output.limit() : "expected output buffer's position="
                + output.position() + " to be at limit" + output.limit(); // output is full
        assert Stream.of(presortedChunks).noneMatch(Buffer::hasRemaining) : "expected all chunks to be drained"; // all elements in scratch space written to output
        assert isSorted(output) : "expected output to be sorted";
        LOGGER.info("wow, the chunks are gone! you really can use coconut oil for everything (merge finished)");
        //endregion
    }

    private void checkMergePreconditions(LongBuffer[] presortedChunks, LongBuffer output) {
        // NOTE: we always get at least 1 chunk because the floor for # of chunks is the # of threads
        var first = presortedChunks[0];
        // NOTE: we have at least 1 element, otherwise we would have thrown the Abort: empty input error
        LOGGER.info("(pre-merge) chunks[0][0]=%d chunks[0][-1]=%d".formatted(
                first.get(0), first.get(first.limit() - 1)));
        LOGGER.info("(pre-merge) output[0][0]=%d output[0][-1]=%d".formatted(
                output.get(0), // NOTE: we have at least 1 element, otherwise we would have thrown an error earlier
                output.get(output.limit() - 1)));
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
    }
}
