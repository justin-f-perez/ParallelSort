package hw1;

import java.nio.LongBuffer;
import java.util.TreeSet;

import static hw1.Utils.getMaximumTotalChunkMemory;

public class ChunkMerger {
    private final LongBuffer[] presortedChunks;
    private final LongBuffer output;

    public ChunkMerger(LongBuffer[] presortedChunks, LongBuffer output) {
        this.presortedChunks = presortedChunks;
        this.output = output;
    }
    public void merge() {
        TreeSet<LongBuffer> ts = new TreeSet<>();
        for (var chunk : presortedChunks) {
            if (chunk.hasRemaining()) ts.add(chunk);
        }
        for (LongBuffer chunk = ts.pollFirst(); chunk != null; chunk = ts.pollFirst()) {
            if (ts.size() == 0) {
                // only one chunk left, whose elements all >= whatever is already in output; consume the rest
                // interestingly, this also prevents some surprising behavior when the number of chunks in
                // the TreeSet is 1: it takes a really long time. I suspect this behavior is related to the fact
                // that TreeSet orders elements via compareTo() and LongBuffer's implementation bases the comparison
                // on its remaining elements. Still, it's not clear why any comparison would need to happen when
                // there's only one element...
                output.put(chunk);
            } else {
                output.put(chunk.get());
                if (chunk.hasRemaining()) ts.add(chunk);
            }
        }
    }
}
