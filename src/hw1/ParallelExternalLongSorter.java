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
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.*;

// TODO: make sure every closeable resource is getting closed
//      driver currently hangs on garbage collection after the sort is complete
public class ParallelExternalLongSorter {
    public ParallelExternalLongSorter(Path inputPath, Path outputPath, int nThreads) throws IOException, InterruptedException {
        // preconditions:
        //      inputPath points to a readable file that exists and is not a directory
        //      outputPath points to a writeable file path and is not a directory
        assert nThreads >= 1;
        assert Files.isReadable(inputPath);
        validateOutputPath(outputPath);

        // setup
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        Path tempFile = Files.createTempFile("external-sort-scratch-space", ".tmp");
        try (
                FileChannel inputFileChannel = FileChannel.open(inputPath, Set.of(READ));
                FileChannel scratchFileChannel = FileChannel.open(tempFile, Set.of(DELETE_ON_CLOSE, READ, WRITE));
                FileChannel outputFileChannel = FileChannel.open(outputPath, Set.of(READ, WRITE))
        ) {

            debug("preparing chunks");
            long inputSize = inputFileChannel.size();
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

            debug("queueing chunk sorting jobs");
            executor.invokeAll(List.of(chunkSorters));
            debug("merging");
            var outputBuffer = outputFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, inputSize).asLongBuffer();
            merge(scratchChunks, outputBuffer);
            debug("done!");
        }
    }

    private static void debug(String msg) {
        System.out.println(msg);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        //self test
        final int inputLength = (int) Math.pow(10, 2); // unit = # of long values
        final int nThreads = 10;
        final Path inputPath = Paths.get("array.bin");
        final Path outputPath = Paths.get("sorted.bin"); // gets written to project root

        long generatorStart = System.nanoTime();
        new DataFileGenerator(inputPath.toString(), inputLength).generate();
        long generatorStop = System.nanoTime();
        double generatorElapsedSeconds = (double) (generatorStop - generatorStart) / Math.pow(10, 9);
        System.out.printf("Finished writing test file in %,.3f seconds%n", generatorElapsedSeconds);

        new ParallelExternalLongSorter(inputPath, outputPath, nThreads);
        try (var sortedFC = FileChannel.open(outputPath)) {
            assertSorted(sortedFC.map(FileChannel.MapMode.READ_ONLY, 0, (long) inputLength * Long.BYTES).asLongBuffer());
        }
    }

    private static void validateOutputPath(Path outputPath) {
        if (Files.exists(outputPath)) {
            assert !Files.isDirectory(outputPath) && Files.isWritable(outputPath);
        } else {
            Path parent = outputPath.getParent();
            assert Files.exists(parent) && Files.isWritable(parent);
        }
    }

    private static void assertSorted(LongBuffer lb) {
        assert IntStream.range(0, lb.limit() - 1).allMatch(i -> lb.get(i) <= lb.get(i + 1));
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
        } while (expectedPeakMemoryUse > workingMemoryLimit);

        assert count > 0;
        debug("Chunk count:" + count);
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
        debug("working memory limit: " + workingMemoryLimit);
        return workingMemoryLimit;
    }

    private void merge(LongBuffer[] presortedChunks, LongBuffer output) {
        debug("preparing to merge chunks, verifying preconditions");
        // check preconditions
        assert output.position() == 0;  // nothing has been written to output yet
        // output buffer is same size as cumulative size of presortedChunks
        assert output.limit() == Stream.of(presortedChunks).mapToInt(Buffer::limit).sum();
        // nothing has been read from presortedChunks yet
        assert Stream.of(presortedChunks).allMatch(c -> c.position() == 0);
        for (var c : presortedChunks) assertSorted(c);

        PriorityQueue<LongBuffer> minHeap = new PriorityQueue<>(new ChunkHeadComparator());
        // initially populate the heap with any non-empty chunk
        for (var chunk : presortedChunks) {
            if (chunk.hasRemaining()) minHeap.add(chunk);
        }
        debug("merging chunks");
        LongBuffer chunkWithSmallestValue = minHeap.poll();
        while (chunkWithSmallestValue != null) {
            output.put(chunkWithSmallestValue.get()); // side effect: advances position fields of outputBuffer and chunk
            if (chunkWithSmallestValue.hasRemaining()) {
                minHeap.add(chunkWithSmallestValue);  // if this buffer still has elements, add it back into the heap
            }
            chunkWithSmallestValue = minHeap.poll();  // poll() returns null when the heap is empty
        }

        debug("merged chunks, verifying post-conditions");
        // check post conditions
        assert output.position() == output.limit(); // output is full
        assert Stream.of(presortedChunks).noneMatch(Buffer::hasRemaining); // all elements in scratch space written to output
        assertSorted(output);
        debug("merged chunks, post-conditions verified");
    }

    private static class ChunkHeadComparator implements Comparator<LongBuffer> {

        @Override
        public int compare(LongBuffer left, LongBuffer right) {
            return Long.compare(left.get(left.position()), right.get(right.position()));
        }
    }

    private static class ChunkSorter implements Callable<Void> {
        //private class ChunkSorter implements Runnable {
        //private class ChunkSorter extends RecursiveAction {
        private final LongBuffer input;
        private final LongBuffer output;

        // precondition: this.input and this.output are small enough to hold in memory
        public ChunkSorter(LongBuffer input, LongBuffer output) {
            this.input = input;
            this.output = output;
        }

        @Override
        public Void call() throws Exception {
            debug("sorting chunk");
            long[] tmp = new long[input.remaining()];
            input.get(tmp);
            Arrays.sort(tmp);
            output.put(tmp);
            debug("finished sorting chunk, rewinding chunk buffer");
            output.position(0);
            return null;
        }

    }
}
