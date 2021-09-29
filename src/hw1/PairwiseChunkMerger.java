package hw1;

import java.nio.LongBuffer;

public class PairwiseChunkMerger extends ChunkMerger {
    // TODO:
    //   approx. 2 mb rounded down to the nearest multiple of Long.BYTES
    //   final int STEP_SIZE = 2 * MB - ((2* MB) % Long.BYTES);
    //   final long[] stepBuffer = new long[2*STEP_SIZE]; // * 2 so we can hold one step from each src buffer
    LongBuffer x;
    LongBuffer y;

    public PairwiseChunkMerger(LongBuffer[] presortedChunks, LongBuffer output) {
        super(presortedChunks, output);
        assert presortedChunks.length == 2 || presortedChunks.length == 1;
        x = presortedChunks[0];
        if (presortedChunks.length > 1) y = presortedChunks[1];
    }

    void drain(LongBuffer buf) {
        output.put(buf);
    }

    @Override
    void merge() {
        if (y == null || !y.hasRemaining()) {
            drain(x);
            return;
        }
        if (!x.hasRemaining()) {
            drain(y);
            return;
        }
        long xVal = x.get();
        long yVal = y.get();
        while (x.hasRemaining() && y.hasRemaining()) {
            var minRemaining = Math.min(x.remaining(), y.remaining());
            // hoping that using a for-loop will lead to some JIT optimized loop unrolling...
            for (int i = 0; i < minRemaining; i++) {
                if (xVal < yVal) {
                    output.put(xVal);
                    xVal = x.get();
                } else {
                    output.put(yVal);
                    yVal = y.get();
                }
            }
        }
        // if y emptied first: drain x until we can output the last value of y
        while (x.hasRemaining() && xVal < yVal) {
            output.put(xVal);
            xVal = x.get();
        }
        // and vice-versa
        while (y.hasRemaining() && yVal < xVal) {
            output.put(yVal);
            yVal = y.get();
        }
        // clean up those last bits
        if (xVal < yVal) {
            output.put(xVal);
            output.put(yVal);
        } else {
            output.put(yVal);
            output.put(xVal);
        }
        // drain whichever one has remaining elements
        if (x.hasRemaining()) drain(x);
        else if (y.hasRemaining()) drain(y);
    }
}
