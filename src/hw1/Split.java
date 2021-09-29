package hw1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.IntStream;

public class Split implements Comparable<Split>{
    // for use with raw byte buffer
    long bytePosition;
    long byteSize;
    // for use with long buffer
    long index;
    long length;

    public Split(Split left, Split right) {
        this(left.index, left.length + right.length);
        // check that these are two contiguous splits
        assert left.index < right.index;
        assert right.index == left.index + left.length;
    }

    public Split(long index, long length) {
        this.bytePosition = index * Long.BYTES;
        this.byteSize = length * Long.BYTES;
        this.index = index;
        this.length = length;
    }

    @Override
    public String toString() {
        return "Split{bytePosition=" + bytePosition + ", byteSize=" + byteSize + ", index=" + index + ", length=" + length + '}';
    }

    /**
     * @param splits input splits, must be of length >= 1 and in sequence (no overlaps, ordered by index, no "holes")
     * @return array of pairs of splits for merging- unless the length of splits is odd, then the first "pair"
     *      actually contains only one element
     */
    public static ArrayList<Split[]> groupSplits(ArrayList<Split> splits) {
        //region check preconditions
        assert splits.size() >= 1;
        assert splits.get(0).index == 0;
        for (int i = 0; i < splits.size() - 1; i++) {
            var x = splits.get(i);
            var y = splits.get(i+1);
            assert x.length <= y.length && x.index <= y.index && x.index + x.length == y.index : ""+x+y;
        }
        //endregion

        ArrayList<Split[]> splitPairs = new ArrayList<>();
        var it = splits.iterator();
        if (splits.size() % 2 == 1) {
            splitPairs.add(new Split[]{it.next()});
        }
        while (it.hasNext()) {
            splitPairs.add(new Split[]{it.next(), it.next()});
        }
        return splitPairs;
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

    /**
     * Compares this object with the specified object for order.  Returns a
     * negative integer, zero, or a positive integer as this object is less
     * than, equal to, or greater than the specified object.
     *
     * <p>The implementor must ensure
     * {@code sgn(x.compareTo(y)) == -sgn(y.compareTo(x))}
     * for all {@code x} and {@code y}.  (This
     * implies that {@code x.compareTo(y)} must throw an exception iff
     * {@code y.compareTo(x)} throws an exception.)
     *
     * <p>The implementor must also ensure that the relation is transitive:
     * {@code (x.compareTo(y) > 0 && y.compareTo(z) > 0)} implies
     * {@code x.compareTo(z) > 0}.
     *
     * <p>Finally, the implementor must ensure that {@code x.compareTo(y)==0}
     * implies that {@code sgn(x.compareTo(z)) == sgn(y.compareTo(z))}, for
     * all {@code z}.
     *
     * <p>It is strongly recommended, but <i>not</i> strictly required that
     * {@code (x.compareTo(y)==0) == (x.equals(y))}.  Generally speaking, any
     * class that implements the {@code Comparable} interface and violates
     * this condition should clearly indicate this fact.  The recommended
     * language is "Note: this class has a natural ordering that is
     * inconsistent with equals."
     *
     * <p>In the foregoing description, the notation
     * {@code sgn(}<i>expression</i>{@code )} designates the mathematical
     * <i>signum</i> function, which is defined to return one of {@code -1},
     * {@code 0}, or {@code 1} according to whether the value of
     * <i>expression</i> is negative, zero, or positive, respectively.
     *
     * @param that the object to be compared.
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     * @throws NullPointerException if the specified object is null
     * @throws ClassCastException   if the specified object's type prevents it
     *                              from being compared to this object.
     */
    @Override
    public int compareTo(Split that) {
        long[] x = {this.index, this.length};
        long[] y = {that.index, that.length};
        return Arrays.compare(x,y);
    }
}
