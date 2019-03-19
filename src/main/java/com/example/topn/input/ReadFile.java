package com.example.topn.input;

import com.example.topn.common.Constants;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Observable;

public class ReadFile extends Observable {
    private static final int NIO_READ_BUF_SIZE = 8192;

    // 换行符
    private byte key = Constants.LINE_SPERATOR.getBytes()[0];

    // 当前行数
    private long lineNum = 0;

    // 文件编码,默认为UTF-8
    private String encode = Constants.FILE_ENCODE;

    // 具体业务逻辑监听器
    private ReadFileListener readerListener;

    /**
     * 获取准确开始位置
     *
     * @param file
     * @param position
     * @return
     * @throws Exception
     */
    public long getStartIndex(File file, long position) throws Exception {
        long startIndex = position;
        FileChannel fc = new RandomAccessFile(file, "r").getChannel();
        fc.position(position);
        ByteBuffer rBuffer = ByteBuffer.allocate(NIO_READ_BUF_SIZE);
        // 每次读取的内容
        byte[] bs = new byte[NIO_READ_BUF_SIZE];
        // 缓存
        byte[] tempBs = new byte[0];
        while (fc.read(rBuffer) != -1) {
            int rSize = rBuffer.position();
            rBuffer.rewind();
            rBuffer.get(bs);
            rBuffer.clear();
            byte[] newStrByte = bs;
            // 如果发现有上次未读完的缓存,则将它加到当前读取的内容前面
            if (null != tempBs) {
                int tL = tempBs.length;
                newStrByte = new byte[rSize + tL];
                System.arraycopy(tempBs, 0, newStrByte, 0, tL);
                System.arraycopy(bs, 0, newStrByte, tL, rSize);
            }
            // 获取开始位置之后的第一个换行符
            int firstNewLineIndex = indexOfNewLine(newStrByte, 0);
            if (firstNewLineIndex != -1) {
                return startIndex + firstNewLineIndex;
            }
            tempBs = substring(newStrByte, 0, newStrByte.length);
            startIndex += NIO_READ_BUF_SIZE;
        }

        fc.close();
        return position;
    }

    /**
     * 从设置的开始位置读取文件，一直到结束为止。如果 end设置为负数,刚读取到文件末尾
     *
     * @param f
     * @param start
     * @param end
     * @throws Exception
     */
    public void readFileByLine(File f, long start, long end) throws Exception {
        FileChannel fc = new RandomAccessFile(f, "r").getChannel();
        fc.position(start);

        ByteBuffer buffer = ByteBuffer.allocate(NIO_READ_BUF_SIZE);
        byte[] bytes = new byte[NIO_READ_BUF_SIZE];
        byte[] tmpBytes = new byte[0];
        long curIndex = start; // 当前读取文件位置

        while (fc.read(buffer) != -1) {
            curIndex += NIO_READ_BUF_SIZE;
            int readSize = buffer.position();
            buffer.rewind();
            buffer.get(bytes);
            buffer.clear();
            byte[] newStrBytes = bytes;

            // 如果发现有上次未读完的缓存,则将它加到当前读取的内容前面
            if (null != tmpBytes) {
                int tmpBytesLength = tmpBytes.length;
                newStrBytes = new byte[readSize + tmpBytesLength];
                System.arraycopy(tmpBytes, 0, newStrBytes, 0, tmpBytesLength);
                System.arraycopy(bytes, 0, newStrBytes, tmpBytesLength, readSize);
            }

            // 是否已经读到最后一位
            boolean isEnd = false;
            // 如果当前读取的位数已经比设置的结束位置大的时候，将读取的内容截取到设置的结束位置
            if (end > 0 && curIndex > end) {
                // 缓存长度 - 当前已经读取位数 - 最后位数
                int l = newStrBytes.length - (int) (curIndex - end);
                newStrBytes = substring(newStrBytes, 0, l);
                isEnd = true;
            }

            int fromIndex = 0, endIndex;
            // 每次读一行内容，以 key（默认为\n） 作为结束符
            while ((endIndex = indexOfNewLine(newStrBytes, fromIndex)) != -1) {
                byte[] bLine = substring(newStrBytes, fromIndex, endIndex);
                String line = new String(bLine, 0, bLine.length, encode);
                lineNum++;
                // 输出一行内容，处理方式由调用方提供
                readerListener.output(line, lineNum, false);
                fromIndex = endIndex + 1;
            }

            // 将未读取完的内容放到缓存中
            tmpBytes = substring(newStrBytes, fromIndex, newStrBytes.length);
            if (isEnd) {
                break;
            }
        }

        // 将剩下的最后内容作为一行，输出，并指明这是最后一行
        String lineStr = new String(tmpBytes, 0, tmpBytes.length, encode);
        readerListener.output(lineStr, lineNum, end < 0 ? true : false);

        fc.close();
        // 通知观察者,当前工作已经完成
        setChanged();
        notifyObservers(start + "-" + end);
    }

    /**
     * 查找一个byte[]从指定位置之后的一个换行符位置
     *
     * @param src
     * @param fromIndex
     * @return
     * @throws Exception
     */
    private int indexOfNewLine(byte[] src, int fromIndex) throws Exception {
        for (int i = fromIndex; i < src.length; i++) {
            if (src[i] == key) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 从指定开始位置读取一个byte[]直到指定结束位置为止生成一个全新的byte[]
     *
     * @param src
     * @param fromIndex
     * @param endIndex
     * @return
     * @throws Exception
     */
    private byte[] substring(byte[] src, int fromIndex, int endIndex) throws Exception {
        int size = endIndex - fromIndex;
        byte[] ret = new byte[size];
        System.arraycopy(src, fromIndex, ret, 0, size);
        return ret;
    }

    public long getLineNum() {
        return lineNum;
    }

    public void setEncode(String encode) {
        this.encode = encode;
    }

    public void setReaderListener(ReadFileListener readerListener) {
        this.readerListener = readerListener;
    }
}
