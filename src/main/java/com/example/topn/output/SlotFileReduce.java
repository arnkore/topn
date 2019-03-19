package com.example.topn.output;

import com.example.topn.common.Constants;
import com.example.topn.direct.DirectListener;
import com.example.topn.input.ReadUtil;
import com.example.topn.merge.MergeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlotFileReduce {
    private static final Logger logger = LoggerFactory.getLogger(SlotFileReduce.class);

    private MergeHandler mergeHandler;

    private DirectListener rfListener = new DirectListener(Constants.FILE_ENCODE);

    public SlotFileReduce(MergeHandler mergeHandler) {
        this.mergeHandler = mergeHandler;
    }

    public void processSlot(int slot) {
        // 处理文件倾斜的情况
        // mergeSplitFiles();
        readFile(slot);
        mergeHandler.addPartialTopN(rfListener.topN(Constants.N));
    }

    /**
     * 读取该槽对应的文件
     */
    private void readFile(int slot) {
        String filename = String.format(Constants.OUTPUT_FILE_TEMPLATE, Constants.OUTPUT_DIR, slot);
        ReadUtil.read(filename, rfListener, 5);
    }
}
