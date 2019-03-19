package com.example.topn.input;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.CountDownLatch;

public class ReadFileThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(ReadFileThread.class);

    private String filePath;

    private long start;

    private long end;

    private ReadFileListener readFileListener;

    private CountDownLatch readOverLatch;

    public ReadFileThread(String file, long start, long end,
                          ReadFileListener readFileListener, CountDownLatch readOverLatch) {
        this.setName("ReadFile" + this.getName());
        this.start = start;
        this.end = end;
        this.filePath = file;
        this.readFileListener = readFileListener;
        this.readOverLatch = readOverLatch;
    }

    @Override
    public void run() {
        ReadFile readFile = new ReadFile();
        readFile.setReaderListener(readFileListener);
        readFile.setEncode(readFileListener.getEncode());
//        readFile.addObserver();

        try {
            File f = new File(filePath);
            if (f.exists()) {
                long startTime = System.currentTimeMillis();
                readFile.readFileByLine(f, start, end + 1);
                long endTime = System.currentTimeMillis();
                String msgTemplate = "%s读取数据用时%.3f秒, 共读取%d行数据。";
                double costTime = (endTime - startTime) / 1000.0;
                logger.info(String.format(msgTemplate, this.getName(), costTime, readFile.getLineNum()));
            } else {
                throw new FileNotFoundException("没有找到文件：" + filePath);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }

        readOverLatch.countDown();
    }
}
