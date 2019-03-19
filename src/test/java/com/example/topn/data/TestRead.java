package com.example.topn.data;

import java.io.*;

public class TestRead {
    public static void main(String[] args) throws IOException {
        long startTime = System.currentTimeMillis();
        String inputPath = "/Users/liuzonghao/Downloads/topn/input.dat";
        BufferedReader br = new BufferedReader(new FileReader(new File(inputPath)));
        String line;
        int lineNum = 0;

        while ((line = br.readLine()) != null) {
            lineNum++;
        }

        long endTime = System.currentTimeMillis();
        System.out.println("用时" + (endTime - startTime) / 1000 + "s, 读取了" + lineNum + "行。");
    }
}
