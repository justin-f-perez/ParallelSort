// Author: Justin Perez

// TIP: cmd + . to (un)fold a //region

//region imports
package hw1;

import java.nio.Buffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;
//endregion

public class ParallelExternalLongSorter {

    //region constants
    static final int THREADPOOL_TIMEOUT_SECONDS = 60;
    static final String DEFAULT_INPUT_FILENAME = "array.bin";
    static final String DEFAULT_OUTPUT_FILENAME = "sorted.bin";
    static final int DEFAULT_NTHREADS = Runtime.getRuntime().availableProcessors();
    private static final int BASE_CHUNK_MULTIPLER = 1;
    //endregion

    public ParallelExternalLongSorter(Path inputPath, Path outputPath, int nThreads) throws Exception {
        //region pre-condition verification (and delete old output)
        debug("either lying to you or verifying constructor args (enable assertions, add '-ea' in your JVM opts)");
        assert nThreads >= 1 : "must have at least 1 thread, not " + nThreads;
        assert !Files.isDirectory(inputPath) : "check yourself before you directoryour self";
        assert Files.isReadable(inputPath) : "input file is not readable";
        var oldOutputDeleted = outputPath.toFile().delete();
        debug((oldOutputDeleted ? "get that nasty mess out of here " : "it was like that when i got here!") + outputPath);
        validateOutputPath(outputPath);
        //endregion

        debug("starting setup");
        Path tempFile = Files.createTempFile("external-sort-scratch-space", ".tmp");
        try (
                FileChannel inputFileChannel = FileChannel.open(inputPath, Set.of(READ));
                FileChannel scratchFileChannel = FileChannel.open(tempFile, Set.of(DELETE_ON_CLOSE, READ, WRITE));
                FileChannel outputFileChannel = FileChannel.open(outputPath, Set.of(CREATE, READ, WRITE))
        ) {
            //region make output files same size as input size
            debug("JUST GIMME SOME ROOM TO BREATHE (preparing scratch space)");
            long inputSize = inputFileChannel.size();
            scratchFileChannel.truncate(inputSize);
            outputFileChannel.truncate(inputSize);
            //endregion

            //region special case for T=1 (sort directly into the output file)
            var didShortcut = tryShortcut(nThreads, inputFileChannel, outputFileChannel);
            if (didShortcut) {
                debug("2ez, im going home early");
                return;
            }
            //endregion

            //region plan where to split the input file
            debug("can longs get covid? better put them in pods just to be safe (preparing chunks)");
            int chunkCount = getChunkCount(nThreads, inputSize);
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
            debug("these sheets are so soft! just look at that thread count: " + nThreads);
            debug("is this expired? its getting chunky (queueing chunk sort jobs)");
            ExecutorService executor = Executors.newFixedThreadPool(nThreads);
            executor.invokeAll(List.of(chunkSorters));
            debug("YOU'RE GONNA BE FRIENDS WHETHER YOU LIKE IT OR NOT! (joining threads)");
            executor.shutdown();  //tell the pool we're done giving it work
            debug("GET TO THE CHOPPA! (awaiting threadpool termination)");
            // blocks until all tasks complete
            boolean timedOut = !executor.awaitTermination(THREADPOOL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (timedOut) throw new RuntimeException("threadpool timed out");
            //endregion

            //region lock files, make scratch file chunks
            var scratchLock = scratchFileChannel.lock(0, scratchFileChannel.size(), false);
            var outputLock= outputFileChannel.lock(0, outputFileChannel.size(), false);
            var scratchChunks = new LongBuffer[(int)splits.length];
            for (int i = 0; i < splits.length; i++) {
                scratchChunks[i] = scratchFileChannel.map(READ_ONLY, splits[i].bytePosition, splits[i].byteSize).asLongBuffer();
            }
            var outputBuffer = outputFileChannel.map(READ_WRITE, 0, inputSize).asLongBuffer();
            //endregion
            //region merge, unlock, make fun of java
            debug("first element (pre-merge)" + outputBuffer.get(0));debug("last" + outputBuffer.get(outputBuffer.limit() - 1));
            merge(scratchChunks, outputBuffer);
            debug("first element (post-merge)" + outputBuffer.get(0));debug("last" + outputBuffer.get(outputBuffer.limit() - 1));
            outputLock.release();
            scratchLock.release();
            debug("doing the world a favor and ending another java process (all done)");
            //endregion
        }
    }

    private boolean tryShortcut(int nThreads, FileChannel inputFileChannel, FileChannel outputFileChannel) throws Exception {
        if (nThreads == 1) {
            ChunkSorter chunkSorter = new ChunkSorter(inputFileChannel, outputFileChannel, new Split(0, inputFileChannel.size()/Long.BYTES));
            chunkSorter.call();
            return true;
        }
        return false;
    }

    //region debug utils
    private static void debug(String msg) {System.out.println("DEBUG [" + System.nanoTime() + "]: " + msg);}
    private static void debug(Object msg) {System.out.println("DEBUG [" + System.nanoTime() + "]: " + msg);}
    //endregion

    public static void main(String[] args) throws Exception {
        //region arg parsing
        long start = System.currentTimeMillis();

        debug(args);
        String inputFileName = (args.length < 1) ? DEFAULT_INPUT_FILENAME : args[0];
        String outputFileName = (args.length < 2) ? DEFAULT_OUTPUT_FILENAME : args[1];
        final int nThreads = (args.length < 3) ? DEFAULT_NTHREADS : Integer.parseInt(args[2]);
        final Path inputPath = Paths.get(inputFileName).toAbsolutePath();
        final Path outputPath = Paths.get(outputFileName).toAbsolutePath();
        debug("you put your long ints in " + inputPath);
        debug("you take your long ints out" + outputPath);
        debug("hardcore, " + DEFAULT_NTHREADS + " cores");
        //endregion

        //region test data generation
        if (args.length >= 4) {
            final int inputLength = Integer.parseInt(args[3]); // unit = # of long values
            long generatorStart = System.nanoTime();
            new DataFileGenerator(inputPath.toString(), inputLength).generate();
            long generatorStop = System.nanoTime();
            double generatorElapsedSeconds = (double) (generatorStop - generatorStart) / Math.pow(10, 9);
            System.out.printf("Finished writing test file with %d long integers in %,.3f seconds%n",
                    inputLength, generatorElapsedSeconds);
        }
        //endregion

        new ParallelExternalLongSorter(inputPath, outputPath, nThreads);
        //region post-condition verification
        try (
                FileChannel inFC = FileChannel.open(inputPath, Set.of(READ));
                FileChannel outFC = FileChannel.open(outputPath, Set.of(READ, WRITE))
        ) {
            assert (inFC.size() == outFC.size()) : "expected in size (" + inFC.size() + ") == out size (" + outFC.size() + ")";
            var in = inFC.map(READ_ONLY, 0, inFC.size()).asLongBuffer();
            var out = outFC.map(READ_ONLY, 0, outFC.size()).asLongBuffer();
            assert isEmpty(in) == isEmpty(out) : "expected output to be all 0's only if input is all 0's";
        }
        //endregion

        long stop = System.currentTimeMillis();

        System.out.printf("Total execution time: %d ms%n", stop - start);
    }

    //region verification utils
    private static void validateOutputPath(Path outputPath) {
        debug("bb pls" + outputPath);
        // cuz if the output file already exists and its already sorted, we can't tell if the program worked or
        // if it just happened to not crash
        assert !outputPath.toFile().exists() : "expected outputPath not to exist";
        Path parent = outputPath.getParent();
        debug("who's ya directory" + parent);
        assert Files.exists(parent) : "parent directory of output path doesn't exist";
        assert Files.isWritable(parent) : "parent directory of output path isn't writable";
    }

    private static boolean isSorted(LongBuffer lb) {
        return IntStream.range(0, lb.limit() - 1).allMatch(i -> lb.get(i) <= lb.get(i + 1));
    }

    private static boolean isEmpty(LongBuffer lb) {
        return IntStream.range(0, lb.limit()).allMatch(i -> lb.get(i) == 0);
    }
    //endregion

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
        int count, chunkMultiplier = BASE_CHUNK_MULTIPLER - 1;

        do {
            chunkMultiplier = chunkMultiplier + 1;
            count = nThreads * chunkMultiplier;
            expectedPeakMemoryUse = inputByteSize / count;
        } while (expectedPeakMemoryUse > workingMemoryLimit && 60000 >= count && count >= 0);

        if (count > 60000) {
            // on most systems Java will choke if you try to do ~64k+ mmaps
            // src: https://mapdb.org/blog/mmap_files_alloc_and_jvm_crash/
            throw new RuntimeException("Aborting: Attempted to allocate too many chunks");
        }

        assert count > 0 : "integer overflow occurred while trying to get chunk count";
        debug("Count Chunkula: " + count);
        return count;
    }
    //endregion

    //region chunk sizing helper (how much room we have for in-memory sorting)
    private long getWorkingMemoryLimit() {
        // final long GB = (long) Math.pow(10, 9); // Gigabyte (SI unit)
        final long MB = (long) Math.pow(10, 6); // Megabyte (SI unit)

        // TODO: do some profiling to figure out how much overhead there really is
        //      looks like there's a 1MB overhead just for creating a thread
        //      https://dzone.com/articles/how-much-memory-does-a-java-thread-take
        // naive implementation - save some space just in case
        final long overhead = 128 * MB;
        //  Maximum heap size: "Smaller of 1/4th of the physical memory or 1GB"
        //      src: https://docs.oracle.com/javase/8/docs/technotes/guides/vm/gc-ergonomics.html
        var workingMemoryLimit = Runtime.getRuntime().maxMemory() - overhead;
        debug("pepperidge farm remembers (MB): " + (workingMemoryLimit / MB));
        return workingMemoryLimit;
    }
    //endregion

    // TODO: too slow
    private void merge(LongBuffer[] presortedChunks, LongBuffer output) {
        //region precondition verification
        debug("applying coconut oil (verifying merge preconditions)");
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
        PriorityQueue<LongBuffer> minHeap = new PriorityQueue<>(new ChunkHeadComparator());
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
        debug("wow, the chunks are gone! you really can use coconut oil for everything (merge finished)");
        //endregion
    }

    //region merge util
    private static class ChunkHeadComparator implements Comparator<LongBuffer> {
        @Override
        public int compare(LongBuffer left, LongBuffer right) {
            return Long.compare(left.get(left.position()), right.get(right.position()));
        }
    }
    //endregion

    //region chunk sort
    private static final class ChunkSorter implements Callable<Void> {
        private final FileChannel inputFileChannel;
        private final FileChannel scratchFileChannel;
        private final Split split;

        private ChunkSorter(FileChannel inputFileChannel, FileChannel scratchFileChannel, Split split) {
            this.inputFileChannel = inputFileChannel;
            this.scratchFileChannel = scratchFileChannel;
            this.split = split;
        }

        @Override
        public Void call() throws Exception {
            var input = inputFileChannel.map(READ_ONLY, split.bytePosition, split.byteSize).asLongBuffer();
            var scratch = scratchFileChannel.map(READ_WRITE, split.bytePosition, split.byteSize).asLongBuffer();
            assert input.position() == scratch.position() : "expected chunk in/out to have same position";
            assert input.limit() == scratch.limit() : "expected chunk in/out to have same limit";
            debug("preparing outdated VCR references (sorting chunk)");
            scratch.mark();
            long[] tmp = new long[input.remaining()];
            input.get(tmp);
            Arrays.sort(tmp);
            scratch.put(tmp);
            debug("be kind, rewind (finished sorting chunk, rewinding chunk buffer)");
            scratch.reset();
            return null;
        }

        public FileChannel inputFileChannel() {
            return inputFileChannel;
        }

        public FileChannel scratchFileChannel() {
            return scratchFileChannel;
        }

        public Split split() {
            return split;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (ChunkSorter) obj;
            return Objects.equals(this.inputFileChannel, that.inputFileChannel) &&
                    Objects.equals(this.scratchFileChannel, that.scratchFileChannel) &&
                    Objects.equals(this.split, that.split);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inputFileChannel, scratchFileChannel, split);
        }

        @Override
        public String toString() {
            return "ChunkSorter[" +
                    "inputFileChannel=" + inputFileChannel + ", " +
                    "scratchFileChannel=" + scratchFileChannel + ", " +
                    "split=" + split + ']';
        }

    }
    //endregion
}
