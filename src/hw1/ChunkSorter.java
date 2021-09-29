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
    private static final Logger LOGGER = Logger.getLogger(CommandLineInterface.class.getName());
    private final FileChannel inputFileChannel;
    private final FileChannel outputFileChannel;
    private final Split split;

    public ChunkSorter(FileChannel inputFileChannel, FileChannel outputFileChannel, Split split) {
        this.inputFileChannel = inputFileChannel;
        this.outputFileChannel = outputFileChannel;
        this.split = split;
    }

    @Override
    public Void call() throws Exception {
        sort();
        return null;
    }

    private void sort() throws IOException {
        var input = inputFileChannel.map(READ_ONLY, split.bytePosition, split.byteSize).asLongBuffer();
        var output = outputFileChannel.map(READ_WRITE, split.bytePosition, split.byteSize).asLongBuffer();
        assert input.position() == output.position() : "expected chunk in/out to have same position";
        assert input.limit() == output.limit() : "expected chunk in/out to have same limit";
        LOGGER.info("preparing outdated VCR references (sorting chunk)");
        output.mark();
        long[] tmp = new long[input.remaining()];
        input.get(tmp);
        Arrays.sort(tmp);
        output.put(tmp);
        LOGGER.info("be kind, rewind (finished sorting chunk, rewinding chunk buffer)");
        output.reset();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (ChunkSorter) obj;
        return Objects.equals(this.inputFileChannel, that.inputFileChannel) &&
                Objects.equals(this.outputFileChannel, that.outputFileChannel) &&
                Objects.equals(this.split, that.split);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inputFileChannel, outputFileChannel, split);
    }

    @Override
    public String toString() {
        return "ChunkSorter[" +
                "inputFileChannel=" + inputFileChannel + ", " +
                "scratchFileChannel=" + outputFileChannel + ", " +
                "split=" + split + ']';
    }
}