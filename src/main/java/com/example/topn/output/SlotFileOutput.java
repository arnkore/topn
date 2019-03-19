package com.example.topn.output;

import com.example.topn.common.Constants;
import com.example.topn.common.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

public class SlotFileOutput {
    private static final Logger logger = LoggerFactory.getLogger(SlotFileOutput.class);

    private static final byte[] KV_SPERATOR_BYTES = Constants.KV_SPERATOR.getBytes();

    private static final byte[] NEWLINE_BYTES = Constants.LINE_SPERATOR.getBytes();

    private static final int SUFFIX_LEN = KV_SPERATOR_BYTES.length + NEWLINE_BYTES.length;

    private static final int WIRTE_BLOCK_SIZE = 81920;

    private int fileIndex;

    private FileChannel outCh;

    private ByteBuffer buf = ByteBuffer.allocate(WIRTE_BLOCK_SIZE);

    public SlotFileOutput(String outputDirectory, int fileIndex) {
        String filename = String.format(Constants.OUTPUT_FILE_TEMPLATE, outputDirectory, fileIndex);
        this.fileIndex = fileIndex;
        try {
            OutputStream outStream = new FileOutputStream(createNewFile(filename));
            outCh = ((FileOutputStream) outStream).getChannel();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            System.exit(ErrorCode.ERR_FILE_NOT_EXISTS);
        }
    }

    /**
     * 判断文件是否存在，若不存在则创建文件
     *
     * @param filepath
     * @return
     * @throws Exception
     */
    private static File createNewFile(String filepath) throws Exception {
        File file = new File(filepath);

        try {
            file.createNewFile();
        } catch (IOException e) {
            logger.error("No such file or directory: " + filepath, e);
        }
        return file;
    }

    public void write(Map<String, Integer> urlMap) {
        try {
            for (Map.Entry<String, Integer> entry : urlMap.entrySet()) {
                String url = entry.getKey();
                int repetitions = entry.getValue();
                byte[] urlBytes = url.getBytes();
                byte[] repetitionsBytes = String.valueOf(repetitions).getBytes();
                if (buf.position() + urlBytes.length + repetitionsBytes.length + SUFFIX_LEN > buf.capacity()) {
                    writeBuf();
                } else {
                    buf.put(url.getBytes());
                    buf.put(KV_SPERATOR_BYTES);
                    buf.put(repetitionsBytes);
                    buf.put(NEWLINE_BYTES);
                }
            }

            writeBuf();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            System.exit(ErrorCode.ERR_WRITE_FILE);
        }
    }

    private void writeBuf() throws IOException {
        buf.flip();
        outCh.write(buf);
        buf.clear();
    }

    public void close() throws IOException {
        outCh.close();
    }

    public int getFileIndex() {
        return fileIndex;
    }
}
