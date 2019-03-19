package com.example.topn.ringbuffer;

import com.example.topn.common.Constants;
import com.example.topn.merge.MergeHandler;
import com.example.topn.output.SlotFileOutput;
import com.example.topn.output.SlotFileReduce;
import com.example.topn.shard.SlotMemMap;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * RingBuffer工厂
 */
public class RingBufferFactory {
    // 指定RingBuffer的大小
    private static final int RING_BUFFER_SIZE = 4096;

    private static final RingBufferEventFactory eventFactory = new RingBufferEventFactory();

    private static final MergeHandler mergeHandler = MergeHandler.getInstance();

    private static SlotFileReduce fileReduce = new SlotFileReduce(mergeHandler);

    /**
     * 创建一个多生产者单消费者RingBuffer
     *
     * @return
     */
    public static RingBuffer<Element> createRingBuffer(int slot) {
        ThreadFactory threadFactory = new RingBufferThreadFactory(slot);
        BlockingWaitStrategy strategy = new BlockingWaitStrategy();
        // 创建disruptor，采用多生产者模式
        Disruptor<Element> disruptor = new Disruptor(eventFactory, RING_BUFFER_SIZE, threadFactory,
                ProducerType.MULTI, strategy);
        ExceptionHandler<Element> exHandler = new FatalExceptionHandler();
        disruptor.handleEventsWith(createEventHandler(slot));
        disruptor.setDefaultExceptionHandler(exHandler);
        disruptor.start();
        return disruptor.getRingBuffer();
    }

    private static EventHandler<Element> createEventHandler(int slot) {
        SlotFileOutput fileOutput = new SlotFileOutput(Constants.OUTPUT_DIR, slot);
        SlotMemMap memMap = new SlotMemMap(fileOutput);
        RingBufferEventHandler eventHandler = new RingBufferEventHandler(memMap, fileReduce);
        return eventHandler;
    }

    /**
     * 创建一个多生产者单消费者RingBuffer
     *
     * @return
     */
    public static RingBuffer<Element> createRingBuffer2(int slot) {
        BlockingWaitStrategy strategy = new BlockingWaitStrategy();
        // 创建disruptor，采用多生产者模式
        RingBuffer<Element> ringBuffer = RingBuffer.create(ProducerType.MULTI, eventFactory, RING_BUFFER_SIZE, strategy);
        SequenceBarrier barrier = ringBuffer.newBarrier();
        SlotConsumer[] consumers = createConsumerGroup(slot, 20);
        ExceptionHandler<Element> exHandler = new FatalExceptionHandler();
        WorkerPool<Element> workerPool = new WorkerPool<>(ringBuffer, barrier, exHandler, consumers);
        ringBuffer.addGatingSequences(workerPool.getWorkerSequences());
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        workerPool.start(executor);
        return ringBuffer;
    }

    private static SlotConsumer[] createConsumerGroup(int slot, int slotWorkers) {
        SlotConsumer[] consumers = new SlotConsumer[slotWorkers];
        for (int i = 0; i < slotWorkers; i++) {
            consumers[i] = new SlotConsumer(slot, i);
        }

        return consumers;
    }

    private static class RingBufferThreadFactory implements ThreadFactory {
        private final int slot;

        public RingBufferThreadFactory(int slot) {
            this.slot = slot;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "RingBuffer" + slot + "-Thread");
        }
    }

    private static class RingBufferEventFactory implements EventFactory<Element> {
        @Override
        public Element newInstance() {
            return new Element();
        }
    }
}
