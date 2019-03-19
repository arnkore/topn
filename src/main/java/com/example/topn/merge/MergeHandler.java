package com.example.topn.merge;

import com.example.topn.common.Constants;
import com.example.topn.heap.HeapElement;
import com.example.topn.heap.MinHeap;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class MergeHandler {
    private static final Logger logger = LoggerFactory.getLogger(MergeHandler.class);

    private List<HeapElement> heapElements = Collections.synchronizedList(Lists.newArrayList());

    private CountDownLatch slotLatch;

    private static volatile MergeHandler mergeHandler;

    private MergeHandler(int slotNums) {
        slotLatch = new CountDownLatch(slotNums);
    }

    public static MergeHandler getInstance() {
        if (mergeHandler == null) {
            synchronized (MergeHandler.class) {
                if (mergeHandler == null) {
                    mergeHandler = new MergeHandler(Constants.AVERAGE_FILE_NUM);
                }
            }
        }

        return mergeHandler;
    }

    public void addPartialTopN(List<HeapElement> partialTopNElements) {
        heapElements.addAll(partialTopNElements);
        slotLatch.countDown();
    }

    public void topN() throws InterruptedException {
        slotLatch.await();
        MinHeap<HeapElement> minHeap = new MinHeap<>(Constants.N);
        for (HeapElement ele : heapElements) {
            if (minHeap.size() < Constants.N) {
                minHeap.insert(ele);
            } else {
                minHeap.replaceMin(ele);
            }
        }

        for (HeapElement ele : minHeap) {
            logger.info(ele.getUrl() + "\t" + ele.getRepetitions());
        }
    }
}
