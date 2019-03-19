package com.example.topn.common;

public class Constants {
    public static final String LINE_SPERATOR = System.getProperty("line.separator");

    public static final String KV_SPERATOR = " ";

    public static final String FILE_ENCODE = "UTF-8";

    public static final String OUTPUT_DIR = "/Users/liuzonghao/Downloads/topn/output";

    public static final String OUTPUT_FILE_TEMPLATE = "%s/output%s.dat";

    // 100GB的文件，分为100个文件，平均每个文件0.5GB。
    public static final int AVERAGE_FILE_NUM = 20;

    // 读文件线程数
    public static final int READ_FILE_THREAD_NUM = 20;

    public static final int N = 100;
}
