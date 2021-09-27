package hw1;

import java.nio.LongBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.IntStream;

class Utils {
    public static boolean isSorted(LongBuffer lb) {
        return IntStream.range(0, lb.limit() - 1).allMatch(i -> lb.get(i) <= lb.get(i + 1));
    }

    public static boolean allSorted(LongBuffer[] chunks){
        return Arrays.stream(chunks).allMatch(Utils::isSorted);
    }

    public static boolean isEmpty(LongBuffer lb) {
        return IntStream.range(0, lb.limit()).allMatch(i -> lb.get(i) == 0);
    }

    public static void validateOutputPath(Path outputPath) {
        // cuz if the output file already exists and its already sorted, we can't tell if the program worked or
        // if it just happened to not crash
        assert !outputPath.toFile().exists() : "expected outputPath not to exist";
        Path parent = outputPath.getParent();
        assert Files.exists(parent) : "parent directory of output path doesn't exist";
        assert Files.isWritable(parent) : "parent directory of output path isn't writable";
    }

    //region merge util
    public static class ChunkHeadComparator implements Comparator<LongBuffer> {
        @Override
        public int compare(LongBuffer left, LongBuffer right) {
            return Long.compare(left.get(left.position()), right.get(right.position()));
        }
    }
    //endregion
}
