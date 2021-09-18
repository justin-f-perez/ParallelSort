package hw1;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.util.Random;
import java.util.stream.LongStream;

public class DatasetGenerator {
    public static void main(String[] args) throws java.io.IOException {
        String outputFileName = (args.length < 1) ? "array.bin" : args[0];
        int N = (args.length < 2) ? 200 : Integer.parseInt(args[1]);

        LongStream ls = new Random().longs(N);

        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(outputFileName))) {
            var it = ls.iterator();
            while (it.hasNext()) dos.writeLong(it.next());
        }
    }
}
