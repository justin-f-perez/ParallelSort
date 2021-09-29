package hw1;

import java.nio.LongBuffer;
import java.util.concurrent.Callable;

public abstract class ChunkMerger implements Callable<Void> {
    final LongBuffer[] presortedChunks;
    final LongBuffer output;

    public ChunkMerger(LongBuffer[] presortedChunks, LongBuffer output) {
        this.presortedChunks = presortedChunks;
        this.output = output;
    }
    abstract void merge();
    public Void call() {
        merge();
        return null;
    }
}
