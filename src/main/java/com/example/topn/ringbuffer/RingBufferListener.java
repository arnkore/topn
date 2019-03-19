package com.example.topn.ringbuffer;

import com.example.topn.input.ReadFileListener;
import com.example.topn.shard.Shard;
import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class RingBufferListener extends ReadFileListener {
    private static final Logger logger = LoggerFactory.getLogger(RingBufferListener.class);

    private Shard shard;

    public RingBufferListener(Shard shard, String encode) {
        Objects.requireNonNull(shard, "shard is null.");
        this.shard = shard;
        setEncode(encode);
    }

    /**
     * 每读取到一行数据，添加到缓存中
     *
     * @param line 读取到的数据
     * @param lineNo 行号
     * @param isEnd 是否读取完成
     * @throws Exception
     */
    public void output(String line, long lineNo, boolean isEnd) throws Exception {
        RingBuffer<Element> ringBuffer = shard.selectRingBuffer(line);
        long sequence = ringBuffer.next();
        try {
            Element ele = ringBuffer.get(sequence);
            ele.setUrl(line);
            ele.setEnd(isEnd);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}
