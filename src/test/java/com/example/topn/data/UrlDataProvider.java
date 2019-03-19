//package com.example.topn.ringbuffer;
//
//import com.example.topn.common.Constants;
//import com.lmax.disruptor.RingBuffer;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.ByteArrayInputStream;
//import java.io.File;
//import java.io.RandomAccessFile;
//import java.nio.MappedByteBuffer;
//import java.nio.channels.FileChannel;
//import java.util.Scanner;
//import java.util.concurrent.CountDownLatch;
//
//public class UrlDataProvider implements Runnable {
//    private static final Logger logger = LoggerFactory.getLogger(UrlDataProvider.class);
//
//    private static final int BUF_SIZE = 0x400000;// 4M的缓冲
//
//    private static final int MAP_BLOCK_SIZE = 0x400000 * 100;
//
//    private String path;
//
//    private RingBuffer<Element> ringBuffer;
//
//    private CountDownLatch mergeLatch;
//
//    public UrlDataProvider(String path, RingBuffer<Element> ringBuffer, CountDownLatch mergeLatch) {
//        this.path = path;
//        this.ringBuffer = ringBuffer;
//        this.mergeLatch = mergeLatch;
//    }
//
//    @Override
//    public void run() {
//        readFile(path);
//    }
//
//    public void readFile(String path) {
//        File file = new File(path);
//        long fileLength = file.length();
//
//        try {
//            // 每次map 100M
//            long startTime = System.currentTimeMillis();
//            long lineNum = 0;
//            for (long startPosition = 0; startPosition < fileLength; startPosition += MAP_BLOCK_SIZE) {
//                boolean lastBlock = (startPosition + MAP_BLOCK_SIZE) > fileLength ? true : false;
//                long realMapSize = (fileLength - startPosition) > MAP_BLOCK_SIZE ?
//                        MAP_BLOCK_SIZE : (fileLength - startPosition);
//                MappedByteBuffer inputBuffer = new RandomAccessFile(file, "r").getChannel()
//                        .map(FileChannel.MapMode.READ_ONLY, startPosition, realMapSize);// 读取大文件
//                byte[] dst = new byte[BUF_SIZE];// 每次读出4M的内容
//
//                for (int offset = 0; offset < realMapSize; offset += BUF_SIZE) {
//                    boolean lastBuffer = lastBlock ? ((offset + BUF_SIZE) > realMapSize ? true : false) : false;
//                    if (realMapSize - offset >= BUF_SIZE) {
//                        for (int i = 0; i < BUF_SIZE; i++) {
//                            dst[i] = inputBuffer.get(offset + i);
//                        }
//                    } else {
//                        dst = new byte[(int)realMapSize - offset];
//                        for (int i = 0; i < realMapSize - offset; i++) {
//                            dst[i] = inputBuffer.get(offset + i);
//                        }
//                    }
//                    // 将得到的4M内容给Scanner，这里的XXX是指Scanner解析的分隔符
//                    Scanner scan = new Scanner(new ByteArrayInputStream(dst)).useDelimiter(Constants.LINE_SPERATOR);
//                    while (scan.hasNext()) {
//                        // 获取下一个可用位置的下标
//                        long sequence = ringBuffer.next();
//                        try {
//                            // 返回可用位置的元素
//                            Element event = ringBuffer.get(sequence);
//                            // 设置该位置元素的值
//                            String url = scan.next();
//                            lineNum++;
//                            event.setUrl(url);
//                            if (lastBuffer && !scan.hasNext()) {
//                                event.setEnd(true);
//                                mergeLatch.countDown();
//                            } else {
//                                event.setEnd(false);
//                            }
//                        } finally {
//                            ringBuffer.publish(sequence);
//                        }
//                    }
//                    scan.close();
//                }
//            }
//
//            long endTime = System.currentTimeMillis();
//            logger.info("读取原始输入文件耗时：" + (endTime - startTime) / 1000 + "ms, 共读取" + lineNum + "行。");
//        } catch (Exception e) {
//            logger.error(e.getMessage(), e);
//        }
//    }
//}
