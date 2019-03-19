package com.example.topn.direct;

import com.example.topn.common.Constants;
import com.example.topn.heap.HeapElement;
import com.example.topn.heap.MinHeap;
import com.example.topn.input.ReadFileListener;
import com.google.common.collect.Lists;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DirectListener extends ReadFileListener {
    private static final Logger logger = LoggerFactory.getLogger(DirectListener.class);

    private Map<String, Integer> urlMapping;

    public DirectListener(String encode) {
        urlMapping = new HashMap<>();
        setEncode(encode);
    }

    @Override
    public void output(String line, long lineNo, boolean isEnd) throws Exception {
        if (StringUtil.isNullOrEmpty(line)) {
            return;
        }
        String[] kvparts = line.split(Constants.KV_SPERATOR);
        String key = kvparts[0];
        Integer repetitions = Integer.parseInt(kvparts[1]);
        Integer value = urlMapping.get(key);
        if (value == null) {
            value = repetitions;
        } else {
            value += repetitions;
        }
        urlMapping.put(key, value);
    }

    /**
     * 计算该槽对应的top N元素列表
     *
     * @param N
     * @return
     */
    public List<HeapElement> topN(int N) {
        MinHeap<HeapElement> minHeap = new MinHeap<>(N);

        for (Map.Entry<String, Integer> entry : urlMapping.entrySet()) {
            String url = entry.getKey();
            Integer repetitions = entry.getValue();
            HeapElement ele = new HeapElement(url, repetitions);
            if (minHeap.size() < N) {
                minHeap.insert(ele);
            } else {
                minHeap.replaceMin(ele);
            }
        }

        urlMapping = new HashMap<>();
        List<HeapElement> res = Lists.newArrayList();
        for (HeapElement ele : minHeap) {
            res.add(ele);
        }

        return res;
    }
}
