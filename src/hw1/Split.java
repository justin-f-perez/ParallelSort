package hw1;

import java.util.Arrays;
import java.util.stream.IntStream;

public class Split {
    // for use with raw byte buffer
    long bytePosition;
    long byteSize;
    // for use with long buffer
    long index;
    long length;

    private Split(long index, long length) {
        this.bytePosition = index * Long.BYTES;
        this.byteSize = length * Long.BYTES;
        this.index = index;
        this.length = length;
    }

    @Override
    public String toString() {
        return "Split{bytePosition=" + bytePosition + ", byteSize=" + byteSize + ", index=" + index + ", length=" + length + '}';
    }

    public static Split[] createSplits(long totalByteSize, int splitCount) {
        // check preconditions: 0 <= totalByteSize && 1 <= splitCount && totalByteSize % Long.BYTES == 0
        assert totalByteSize >= 0 : "totalByteSize must be >= 0";
        assert 1 <= splitCount : "there cannot be fewer than one split";
        assert totalByteSize % Long.BYTES == 0 : "totalByteSize must be a multiple of Long.BYTES";

        long totalLength = totalByteSize / Long.BYTES;
        long typicalSplitLength = totalLength / splitCount;
        long lastSplitLength = typicalSplitLength;
        if (totalLength % splitCount != 0) {
            typicalSplitLength++;
            lastSplitLength = totalLength - (typicalSplitLength * (splitCount - 1));
        }

        Split[] splits = new Split[splitCount];
        long currentIndex = 0;
        for (int i = 0; i < splits.length; i++) {
            long splitLength = (i == splits.length - 1) ? lastSplitLength : typicalSplitLength;

            splits[i] = new Split(currentIndex, splitLength);
            currentIndex += splitLength;
        }

        // check post-conditions
        // all splits have the same size, except the final split which may be slightly smaller than the rest
        Split first = splits[0], last = splits[splits.length - 1];
        assert IntStream.range(0, splits.length - 1).allMatch(i -> splits[i].byteSize == first.byteSize);
        assert first.byteSize >= last.byteSize;

        // our splits have a cumulative size equal to the input total size
        assert Arrays.stream(splits).mapToLong((split) -> split.byteSize).sum() == totalByteSize;

        // we produced the correct number of splits
        // noinspection ConstantConditions
        assert splits.length == splitCount;

        // no overlaps: for any sequential pair s1,s2 s2.index = s1.index + s2.length
        assert splits.length == 1 ||
                IntStream.range(0, splits.length - 1)
                        .allMatch(i -> splits[i + 1].index == splits[i].index + splits[i].length);

        return splits;
    }

    @SuppressWarnings("ConstantConditions")
    public static void main(String[] args) {
        // check that assertions are on
        boolean assertionsEnabled = false;
        try {
            assert false;
        } catch (AssertionError e) {
            assertionsEnabled = true;
        }
        if (!assertionsEnabled) throw new RuntimeException("Enable assertions");
        Split[] splits;

        // test empty splits
        splits = Split.createSplits(0, 100);
        assert Arrays.stream(splits).allMatch(s -> s.index == 0 && s.length == 0);

        // test singular split
        splits = Split.createSplits(Long.BYTES * 100, 1);
        assert (splits.length == 1) && (splits[0].index == 0) && (splits[0].length == 100);

        // test multi-split
        splits = Split.createSplits(Long.BYTES * 100, 2);
        assert (splits.length == 2)
                && (splits[0].index == 0) && (splits[0].length == 50)
                && (splits[1].index == 50) && (splits[1].length == 50);

        // test non-uniform sized splits
        splits = Split.createSplits(Long.BYTES * 100, 3);
        assert (splits.length == 3)
                && (splits[0].index == 0) && (splits[0].length == 34)
                && (splits[1].index == 34) && (splits[1].length == 34)
                && (splits[2].index == 68) && (splits[2].length == 32);

        System.out.println("Self-testing passed.");
    }
}
