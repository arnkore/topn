package com.example.topn.ringbuffer;

import com.example.topn.common.Constants;
import com.example.topn.output.SlotFileReduce;
import com.example.topn.shard.SlotMemMap;
import com.lmax.disruptor.EventHandler;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RingBufferEventHandler implements EventHandler<Element> {
    private static final Logger logger = LoggerFactory.getLogger(RingBufferEventHandler.class);

    private SlotFileReduce fileReduce;

    private SlotMemMap memMap;

    private String lastUrl;

    private int repetitions;

    private int lineNum;

    private int lastSlot;

    public RingBufferEventHandler(SlotMemMap memMap, SlotFileReduce fileReduce) {
        this.memMap = memMap;
        this.fileReduce = fileReduce;
    }

    @Override
    public void onEvent(Element event, long sequence, boolean endOfBatch) throws Exception {
        String url = event.getUrl();
        if (!StringUtil.isNullOrEmpty(url)) {
            lineNum++;
            // 维护一个lastUrl实现请求合并处理
            if (lastUrl == null) {
                lastUrl = url;
                repetitions = 1;
            } else if (lastUrl.equalsIgnoreCase(url)) {
                repetitions++;
            } else {
                memMap.insert(lastUrl, repetitions);
                lastUrl = url;
                repetitions = 1;
            }
        }

        // 处理最后一条记录
        if (event.isEnd()) {
            if (lastUrl != null) {
                memMap.insert(lastUrl, repetitions);
                memMap.write();
                lastUrl = null;
                repetitions = 0;
            }
            Thread t = Thread.currentThread();
            logger.info(t.getName() + "已处理了" + lineNum + "行数据.");
            for (int i = 0; i < Constants.AVERAGE_FILE_NUM; i++) {
                fileReduce.processSlot(i);
            }
        }
    }
}
