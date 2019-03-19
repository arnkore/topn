package com.example.topn.shard;

import com.example.topn.common.Constants;
import com.example.topn.ringbuffer.Element;
import com.example.topn.ringbuffer.RingBufferRepository;
import com.lmax.disruptor.RingBuffer;

import java.util.Objects;

public class Shard {
    private RingBufferRepository rbRepo;

    public Shard(RingBufferRepository rbRepo) {
        Objects.requireNonNull(rbRepo, "ringBufferRepository is null.");
        this.rbRepo = rbRepo;
    }

    private static int computeSlot(String str) {
        return (str.hashCode() & Integer.MAX_VALUE) % Constants.AVERAGE_FILE_NUM;
    }

    public RingBuffer<Element> selectRingBuffer(String str) {
        int slot = computeSlot(str);
        return rbRepo.getRingBuffer(slot);
    }
}
