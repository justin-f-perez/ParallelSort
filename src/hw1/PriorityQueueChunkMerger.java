package hw1;

import java.nio.LongBuffer;
import java.util.PriorityQueue;

public class PriorityQueueChunkMerger extends ChunkMerger {

    PriorityQueue<LongBuffer> minHeap;

    public PriorityQueueChunkMerger(LongBuffer[] presortedChunks, LongBuffer output) {
        super(presortedChunks, output);
        this.minHeap = new PriorityQueue<>(new Utils.ChunkHeadComparator());

    }

    @Override
    void merge() {
        //region implementation
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
    }
}
