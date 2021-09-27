package hw1;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

@SuppressWarnings("ClassCanBeRecord") // suppressed because colab notebook's version of java doesn't support 'record'
class ChunkSorter implements Callable<Void> {
    private final FileChannel inputFileChannel;
    private final FileChannel scratchFileChannel;
    private final Split split;
    private static final Logger LOGGER = Logger.getLogger(CommandLineInterface.class.getName());

    public ChunkSorter(FileChannel inputFileChannel, FileChannel scratchFileChannel, Split split) {
        this.inputFileChannel = inputFileChannel;
        this.scratchFileChannel = scratchFileChannel;
        this.split = split;
    }

    @Override
    public Void call() throws Exception {
        sort();
        return null;
    }

    private void sort() throws IOException {
        var input = inputFileChannel.map(READ_ONLY, split.bytePosition, split.byteSize).asLongBuffer();
        var scratch = scratchFileChannel.map(READ_WRITE, split.bytePosition, split.byteSize).asLongBuffer();
        assert input.position() == scratch.position() : "expected chunk in/out to have same position";
        assert input.limit() == scratch.limit() : "expected chunk in/out to have same limit";
        LOGGER.info("preparing outdated VCR references (sorting chunk)");
        scratch.mark();
        long[] tmp = new long[input.remaining()];
        input.get(tmp);
        Arrays.sort(tmp);
        scratch.put(tmp);
        LOGGER.info("be kind, rewind (finished sorting chunk, rewinding chunk buffer)");
        scratch.reset();
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