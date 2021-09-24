// Author: Justin Perez

package hw1;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.stream.LongStream;

public class DataFileGenerator {
    String filename;
    int N;

    public DataFileGenerator(String filename, int N) {
        this.filename = filename;
        this.N = N;
    }

    public static void main(String[] args) throws java.io.IOException {
        String outputFileName = (args.length < 1) ? "array.bin" : args[0];
        int N = (args.length < 2) ? 100 : Integer.parseInt(args[1]);
        new DataFileGenerator(args[0], Integer.parseInt(args[1])).generate();
    }

    public void generate() throws IOException {
        LongStream ls = new Random().longs(N);
        try (var f = new FileOutputStream(this.filename);
             var dataWriter = new DataOutputStream(f)) {
            var it = ls.iterator();
            while (it.hasNext()) {
                dataWriter.writeLong(it.next());
            }
        }
    }


}
