package com.example.topn.data;

import com.example.topn.util.RandomStringUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class PrepareTestData {
    public static void main(String[] args) throws IOException {
        String filename = "/Users/liuzonghao/Downloads/topn/input3.dat";
        File file = new File(filename);
        file.createNewFile();
        BufferedWriter out = new BufferedWriter(new FileWriter(file));
        Random rd  = new Random();
        final int FLUSH_SIZE = 10000;
        for (int i = 0; i < 100000000; i++) {
            StringBuilder appendable = new StringBuilder();
            appendable.append("www.");
            appendable.append(RandomStringUtils.randomAlphabetic(6));
            appendable.append(".com\n");
            out.write(appendable.toString());

            if (++i >= FLUSH_SIZE) {
                out.flush();
            }
        }
        out.flush();
        out.close();
    }
}
