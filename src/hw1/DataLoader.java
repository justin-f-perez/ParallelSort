package hw1;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

public class DataLoader {
    public static void main(String[] args) throws IOException {
        try (DataInputStream dis = new DataInputStream(new FileInputStream("array.bin"))) {
            while (dis.available() > 0) System.out.println(dis.readLong());
        }
    }
}
