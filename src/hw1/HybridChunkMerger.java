package hw1;

import java.nio.LongBuffer;
import java.util.PriorityQueue;
import java.util.TreeSet;

public class HybridChunkMerger extends ChunkMerger{


    TreeSet<LongBuffer> chunkTreeSet;
    PriorityQueue<Long> minHeap; // <-- see below, need a different implementation of min heap
    public HybridChunkMerger(LongBuffer[] presortedChunks, LongBuffer output) {
        super(presortedChunks, output);
        this.chunkTreeSet = new TreeSet<>();
        this.minHeap = new PriorityQueue<>();
    }

    @Override
    void merge() {
        // TODO:
        //  Conjecture: instead of having MinHeap MH that gets populated with 1 element from each chunk and
        //  taking 1 element at a time we can take any fixed #, CL ("chonk length"), as long as we add CL elements back
        //  into MH from the chunk with the smallest value at its current position
        //  a TreeSet TS can efficiently provide the chunk with the smallest value
        //  ================================================
        //  PS <- presorted chunks
        //  CL <- "chonk length"; chonk = "chunk of chunk", should be some fixed number small enough to prevent OOM errors
        //  remaining(chunks) <- {c: c in presorted chunks and c.hasRemaining()}
        //  TS <- treeset of chunks (as in TreeSetChunkMerger)
        //  MH <- min heap of longs (similar to PriorityQueueChunkMerger*)
        //  OUT <- output
        //  ================================================
        //  for {chunk s.t. chunk in PS and chunk has remaining}
        //       chonk <- take next CL elements from chunk if there are that many, otherwise take all remaining
        //       add chonk to MH
        //       if chunk has remaining:
        //           add chunk to TS
        //  while MH has remaining:
        //       take CL elements from MH, add to OUT
        //       if TS has remaining:
        //           chunk <- take next 1 from TS
        //           chonk <- take next 'CL' elements from chunk if there are that many, otherwise take all remaining
        //           add chonk to MH
        //           if chunk has remaining elements:
        //               add chunk to TS
        //  ==================================================
        //  * the min heap would need an implementation that supports primitives, which PriorityQueue does not, because
        //      boxing/unboxing (long <--> Long) overhead eat too much cpu/mem; PriorityQueue<long> is invalid,
        //      PriorityQueue<Long> will "autobox"
    }
}
