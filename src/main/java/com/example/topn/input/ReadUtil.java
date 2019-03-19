package com.example.topn.input;

import com.example.topn.BootStrap;
import com.example.topn.common.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ReadUtil {
    private static final Logger logger = LoggerFactory.getLogger(BootStrap.class);

    public static void read(String inputFilePath, ReadFileListener listener, int threadNum) {
        File file = new File(inputFilePath);
        try {
            ReadFile rf = new ReadFile();
            long fileLength = file.length();
            CountDownLatch latch = new CountDownLatch(threadNum);
            // 线程粗略开始位置
            long blockRoughSize = fileLength / threadNum;
            long startTime = System.currentTimeMillis();

            for (int j = 0; j < threadNum; j++) {
                // 计算精确开始位置
                long startNum = (j == 0) ? 0 : rf.getStartIndex(file, blockRoughSize * j);
                long endNum = j + 1 < threadNum ? rf.getStartIndex(file, blockRoughSize * (j + 1)) : -2;
                Thread t = new ReadFileThread(file.getPath(), startNum, endNum, listener, latch);
                t.start();
            }
            latch.await();
            long endTime = System.currentTimeMillis();
            logger.info("读取文件" + file.getName() + "总用时" + (endTime - startTime) / 1000 + "s");
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            System.exit(ErrorCode.ERR_READ_FILE);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            System.exit(ErrorCode.ERR_OTHER);
        }
    }
}
