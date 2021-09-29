package hw1;

import java.nio.LongBuffer;
import java.util.TreeSet;

public class TreeSetChunkMerger extends ChunkMerger {
    private final TreeSet<LongBuffer> chunkTreeSet;
    // TODO: performance could be further improved with a hybrid TreeSetPriorityQueueChunkMerger
    //      in which a TreeSet is used for retrieving the

    // "but wait! it's a tree SET! won't we lose duplicate long values?"
    // no, we're storing a unique set of LongBuffer, not the long values themselves
    public TreeSetChunkMerger(LongBuffer[] presortedChunks, LongBuffer output) {
        super(presortedChunks, output);
        this.chunkTreeSet = new TreeSet<>();
    }

    @Override
    public void merge() {
        for (var chunk : presortedChunks) {
            if (chunk.hasRemaining()) chunkTreeSet.add(chunk);
        }
        for (LongBuffer chunk = chunkTreeSet.pollFirst(); chunk != null; chunk = chunkTreeSet.pollFirst()) {
            if (chunkTreeSet.size() == 0) {
                // only one chunk left, whose elements all >= whatever is already in output; consume the rest
                // interestingly, this also prevents some surprising behavior when the number of chunks in
                // the TreeSet is 1: it takes a really long time. I suspect this behavior is related to the fact
                // that TreeSet orders elements via compareTo() and LongBuffer's implementation bases the comparison
                // on its remaining elements. Still, it's not clear why any comparison would need to happen when
                // there's only one element...
                output.put(chunk);
            } else {
                output.put(chunk.get());
                if (chunk.hasRemaining()) chunkTreeSet.add(chunk);
            }
        }
    }

    private void appendFrom() {

    }
}
