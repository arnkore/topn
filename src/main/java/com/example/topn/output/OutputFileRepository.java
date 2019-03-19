package com.example.topn.output;

import com.google.common.collect.Lists;

import java.io.File;
import java.util.List;

public class OutputFileRepository {
    private final List<File> ringBuffers = Lists.newArrayList();

    public OutputFileRepository() {
    }
}
