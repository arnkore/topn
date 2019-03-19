package com.example.topn.ringbuffer;

import com.lmax.disruptor.WorkHandler;

public class SlotConsumer implements WorkHandler<Element> {
    private int slot;

    private int index;

    public SlotConsumer(int slot, int index) {
        this.slot = slot;
        this.index = index;
    }

    @Override
    public void onEvent(Element event) throws Exception {

    }
}
