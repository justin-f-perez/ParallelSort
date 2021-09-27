package hw1;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

public class DataFilePrinter {
    String[] filenames;

    public DataFilePrinter(String[] filenames) {
        this.filenames = filenames;
    }

    public DataFilePrinter(String filename) {
        this(new String[]{filename});
    }

    public static void main(String[] args) throws IOException {
        new DataFilePrinter(args).print();
    }

    public void print() throws IOException {
        for (String filename : filenames) {
            System.out.println(filename);
            try (DataInputStream dis = new DataInputStream(new FileInputStream(filename))) {
                while (dis.available() > 0) System.out.println(dis.readLong());
            }
            System.out.println("End of file.");
        }
        System.out.println("Finished printing all files.");
    }
}
