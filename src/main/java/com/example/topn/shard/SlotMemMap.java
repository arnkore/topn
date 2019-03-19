package com.example.topn.shard;

import com.example.topn.output.SlotFileOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SlotMemMap {
    private static final Logger logger = LoggerFactory.getLogger(SlotMemMap.class);

    private static final int MEM_MAP_CAPACITY = 2000;

    private Map<String, Integer> mutableMemMap = new ConcurrentHashMap();

    private Map<String, Integer> immutableMemMap;

    private long elementsNum;

    private SlotFileOutput fileOutput;

    public SlotMemMap(SlotFileOutput fileOutput) {
        this.fileOutput = fileOutput;
    }

    public void insert(String key, Integer incrDelta) {
        Integer val = mutableMemMap.get(key);
        if (val == null) {
            val = 0;
        }

        val += incrDelta;
        mutableMemMap.put(key, val);

        if (++elementsNum % MEM_MAP_CAPACITY == 0) {
            // 生成新的Immutable Memory map，然后flush到磁盘。
            write();
        }
    }

    public void write() {
        if (!mutableMemMap.isEmpty()) {
            immutableMemMap = mutableMemMap;
            mutableMemMap = new ConcurrentHashMap<>();
            fileOutput.write(immutableMemMap);
        }
    }
}
