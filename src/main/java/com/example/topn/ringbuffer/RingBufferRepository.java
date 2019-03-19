package com.example.topn.ringbuffer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.lmax.disruptor.RingBuffer;

import java.util.List;
import java.util.Objects;

public class RingBufferRepository {
    private final List<RingBuffer<Element>> ringBuffers = Lists.newArrayList();

    private final int ringBufferNumber;

    public RingBufferRepository(RingBufferFactory ringBufferFactory, int ringBufferNumber) {
        Objects.requireNonNull(ringBufferFactory, "ringBufferFactory is null.");
        Preconditions.checkArgument(ringBufferNumber > 0, "ringBufferNumber must be greater than zero.");
        this.ringBufferNumber = ringBufferNumber;

        for (int i = 0; i < ringBufferNumber; i++) {
            RingBuffer<Element> ringBuffer = ringBufferFactory.createRingBuffer(i);
            ringBuffers.add(ringBuffer);
        }
    }

    public RingBuffer<Element> getRingBuffer(int index) {
        if (index < 0 || index > ringBufferNumber) {
            throw new IllegalArgumentException("ringBufferIndex out of bounds, legal range is 0 to " + ringBufferNumber);
        }
        return ringBuffers.get(index);
    }
}
