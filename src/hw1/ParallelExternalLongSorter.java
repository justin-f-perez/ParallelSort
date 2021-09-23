// Author: Justin Perez

package hw1;

import java.io.IOException;
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

import static java.nio.file.StandardOpenOption.*;

// TODO: make sure every closeable resource is getting closed
//      driver currently hangs on garbage collection after the sort is complete
public class ParallelExternalLongSorter {

    static final int THREADPOOL_TIMEOUT_SECONDS = 60;
    static final String DEFAULT_INPUT_FILENAME = "array.bin";
    static final String DEFAULT_OUTPUT_FILENAME = "sorted.bin";
    static final int DEFAULT_NTHREADS = 10;

    public ParallelExternalLongSorter(Path inputPath, Path outputPath, int nThreads) throws IOException, InterruptedException {
        // preconditions:
        //      inputPath points to a readable file that exists and is not a directory
        //      outputPath points to a writeable file path and is not a directory
        debug("either lying to you or verifying constructor args (enable assertions, add '-ea' in your JVM opts)");
        assert nThreads >= 1 : "must have at least 1 thread, not " + nThreads;
        assert Files.isReadable(inputPath): "input file is not readable";
        validateOutputPath(outputPath);

        debug("starting setup");
        Path tempFile = Files.createTempFile("external-sort-scratch-space", ".tmp");
        try (
                FileChannel inputFileChannel = FileChannel.open(inputPath, Set.of(READ));
                FileChannel scratchFileChannel = FileChannel.open(tempFile, Set.of(DELETE_ON_CLOSE, READ, WRITE));
                FileChannel outputFileChannel = FileChannel.open(outputPath, Set.of(READ, WRITE))
        ) {
            debug("JUST GIMME SOME ROOM TO BREATHE (preparing scratch space)");
            long inputSize = inputFileChannel.size();
            scratchFileChannel.truncate(inputSize);
            outputFileChannel.truncate(inputSize);

            debug("can longs get covid? better put them in pods just to be safe (preparing chunks)");
            int chunkCount = getChunkCount(nThreads, inputSize);
            Split[] splits = Split.createSplits(inputSize, chunkCount);
            LongBuffer[] inputChunks = new LongBuffer[chunkCount], scratchChunks = new LongBuffer[chunkCount];
            ChunkSorter[] chunkSorters = new ChunkSorter[chunkCount];
            for (int i = 0; i < splits.length; i++) {
                var split = splits[i];
                inputChunks[i] = inputFileChannel.map(FileChannel.MapMode.READ_ONLY, split.bytePosition, split.byteSize).asLongBuffer();
                scratchChunks[i] = scratchFileChannel.map(FileChannel.MapMode.READ_WRITE, split.bytePosition, split.byteSize).asLongBuffer();
                chunkSorters[i] = new ChunkSorter(inputChunks[i], scratchChunks[i]);
            }
            debug("these sheets are so soft! just look at that thread count: " + nThreads);
            debug("is this expired? its getting chunky (queueing chunk sort jobs)");
            ExecutorService executor = Executors.newFixedThreadPool(nThreads);
            executor.invokeAll(List.of(chunkSorters));
            debug("YOU'RE GONNA BE FRIENDS WHETHER YOU LIKE IT OR NOT! (joining threads)");
            executor.shutdown();
            debug("GET TO THE CHOPPA! (awaiting threadpool termination)");
            boolean timedOut = !executor.awaitTermination(THREADPOOL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (timedOut) throw new RuntimeException("threadpool timed out");

            var outputBuffer = outputFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, inputSize).asLongBuffer();
            debug(""+outputBuffer.get(0));
            debug(""+outputBuffer.get(1));
            debug(""+outputBuffer.get(outputBuffer.limit() - 2));
            debug(""+outputBuffer.get(outputBuffer.limit() - 1));
            merge(scratchChunks, outputBuffer);
            debug(""+outputBuffer.get(0));
            debug(""+outputBuffer.get(1));
            debug(""+outputBuffer.get(outputBuffer.limit() - 2));
            debug(""+outputBuffer.get(outputBuffer.limit() - 1));
            debug("kthxbye!");
        }
    }

    private static void debug(String msg) {System.out.println("DEBUG [" + System.nanoTime() + "]: " + msg);}

    public static void main(String[] args) throws IOException, InterruptedException {
        // zZzZzZ parsing args
        String inputFileName = (args.length < 1) ? DEFAULT_INPUT_FILENAME : args[0];
        String outputFileName = (args.length < 2) ? DEFAULT_OUTPUT_FILENAME : args[1];
        final int nThreads = (args.length < 3) ? DEFAULT_NTHREADS : Integer.parseInt(args[2]);
        final Path inputPath = Paths.get(inputFileName);
        final Path outputPath = Paths.get(outputFileName);

        if (args.length >= 4) {
            final int inputLength = Integer.parseInt(args[3]); // unit = # of long values
            long generatorStart = System.nanoTime();
            new DataFileGenerator(inputPath.toString(), inputLength).generate();
            long generatorStop = System.nanoTime();
            double generatorElapsedSeconds = (double) (generatorStop - generatorStart) / Math.pow(10, 9);
            System.out.printf("Finished writing test file with %d long integers in %,.3f seconds%n",
                    inputLength, generatorElapsedSeconds);
        }

        // time for magic
        new ParallelExternalLongSorter(inputPath, outputPath, nThreads);
        try(
            FileChannel inFC = FileChannel.open(inputPath, Set.of(READ));
            FileChannel outFC = FileChannel.open(outputPath, Set.of(READ, WRITE))
        ){
            assert (inFC.size() == outFC.size()) : "expected in size (" + inFC.size() + ") == out size (" + outFC.size() + ")";
            var in = inFC.map(FileChannel.MapMode.READ_ONLY, 0, inFC.size()).asLongBuffer();
            var out= outFC.map(FileChannel.MapMode.READ_ONLY, 0, outFC.size()).asLongBuffer();
            assert isEmpty(in) == isEmpty(out) : "expected output to be all 0's only if input is all 0's";
        }
    }

    private static void validateOutputPath(Path outputPath) {
        if (Files.exists(outputPath)) {
            assert !Files.isDirectory(outputPath) : "output path should be a file, not dir";
            assert Files.isWritable(outputPath) : "output path already exists and isn't writable";
        } else {
            Path parent = outputPath.getParent();
            assert Files.exists(parent) : "parent directory of output path doesn't exist" ;
            assert Files.isWritable(parent) : "parent directory of output path isn't writable";
        }
    }

    private static boolean isSorted(LongBuffer lb) {
        return IntStream.range(0, lb.limit() - 1).allMatch(i -> lb.get(i) <= lb.get(i + 1));
    }

    private static boolean isEmpty(LongBuffer lb) {
        return IntStream.range(0, lb.limit()).allMatch(i -> lb.get(i) == 0);
    }

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
        int count, chunkMultiplier = 0;

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

    // returns the amount of RAM we can safely use without causing OOM errors
    private long getWorkingMemoryLimit() {
        // naive implementation
        final long GB = (long) Math.pow(10, 9); // Gigabyte (SI unit)
        final long MB = (long) Math.pow(10, 6); // Megabyte (SI unit)
        // TODO: implement a real check to see how much memory we have, 1 GB is a conservative figure as we expect this
        //  to be run on a machine with >4GB RAM and with default JVM arguments
        //  Maximum heap size: "Smaller of 1/4th of the physical memory or 1GB"
        //      src: https://docs.oracle.com/javase/8/docs/technotes/guides/vm/gc-ergonomics.html
        @SuppressWarnings("PointlessArithmeticExpression") final long jvmMaxHeap = 1 * GB;
        // TODO: do some profiling to figure out how much overhead there really is
        //      looks like there's a 1MB overhead just for creating a thread
        //      https://dzone.com/articles/how-much-memory-does-a-java-thread-take
        final long overhead = 128 * MB;
        var workingMemoryLimit = jvmMaxHeap - overhead;
        debug("working memory limit (MB): " + (workingMemoryLimit / MB));
        return workingMemoryLimit;
    }

    private void merge(LongBuffer[] presortedChunks, LongBuffer output) {
        debug("applying coconut oil (verifying merge preconditions)");
        // check preconditions
        assert output.position() == 0 : "expected output buffer to start at position 0";  // nothing has been written to output yet
        // output buffer is same size as cumulative size of presortedChunks
        assert output.limit() == Stream.of(presortedChunks).mapToInt(Buffer::limit).sum() : "expected output & scratch chunks to be same size";
        // nothing has been read from presortedChunks yet

        // TODO: this should actually fail every time, verify and delete this line
        assert Stream.of(presortedChunks).allMatch(c -> c.position() == 0) : "expected scratch chunks to be at position 0";

        for (var c : presortedChunks) assert isSorted(c) : "expected scratch chunks to be pre-sorted";

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

        // check post conditions
        assert output.position() == output.limit() : "expected output buffer's position to be at limit"; // output is full
        assert Stream.of(presortedChunks).noneMatch(Buffer::hasRemaining) : "expected all chunks to be drained"; // all elements in scratch space written to output
        assert isSorted(output) : "expected output to be sorted";
        debug("wow, the chunks are gone! you really can use coconut oil for everything (merge finished)");
    }

    private static class ChunkHeadComparator implements Comparator<LongBuffer> {

        @Override
        public int compare(LongBuffer left, LongBuffer right) {
            return Long.compare(left.get(left.position()), right.get(right.position()));
        }
    }

    private record ChunkSorter(LongBuffer input, LongBuffer output) implements Callable<Void> {
        // precondition: this.input and this.output are small enough to hold in memory

        @Override
        public Void call() throws Exception {
            assert input.position() == output.position() : "expected chunk in/out to have same position";
            assert input.limit() == output.limit() : "expected chunk in/out to have same limit";
            debug("preparing outdated VCR references (sorting chunk)");
            output.mark();
            long[] tmp = new long[input.remaining()];
            input.get(tmp);
            Arrays.sort(tmp);
            output.put(tmp);
            debug("be kind, rewind (finished sorting chunk, rewinding chunk buffer)");
            output.reset();
            return null;
        }

    }
}
