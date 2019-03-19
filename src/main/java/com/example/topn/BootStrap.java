package com.example.topn;

import com.example.topn.common.Constants;
import com.example.topn.input.ReadFileListener;
import com.example.topn.input.ReadUtil;
import com.example.topn.merge.MergeHandler;
import com.example.topn.ringbuffer.RingBufferFactory;
import com.example.topn.ringbuffer.RingBufferListener;
import com.example.topn.ringbuffer.RingBufferRepository;
import com.example.topn.shard.Shard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author nevermore
 * @date 2019/3/13 上午10:52
 */
public class BootStrap {
    private static final Logger logger = LoggerFactory.getLogger(BootStrap.class);

    private static final String inputFilePath = "/Users/liuzonghao/Downloads/topn/input.dat";

    public static void main(String[] args) throws Exception {
        logger.info("BootStrap!!!");

        final int splitFilesNum = Constants.AVERAGE_FILE_NUM;
        RingBufferFactory rbFactory = new RingBufferFactory();
        RingBufferRepository rbRepo = new RingBufferRepository(rbFactory, splitFilesNum);
        Shard shard = new Shard(rbRepo);
        ReadFileListener listener = createRingBufferListener(shard);
        ReadUtil.read(inputFilePath, listener, Constants.READ_FILE_THREAD_NUM);
        MergeHandler merge = MergeHandler.getInstance();
        merge.topN();
        System.exit(0);
    }

    private static ReadFileListener createRingBufferListener(Shard shard) {
        ReadFileListener listener = new RingBufferListener(shard, Constants.FILE_ENCODE);
        return listener;
    }
}
