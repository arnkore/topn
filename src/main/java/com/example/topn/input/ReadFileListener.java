package com.example.topn.input;


public abstract class ReadFileListener {
    private String encode;

    public String getEncode() {
        return encode;
    }

    public void setEncode(String encode) {
        this.encode = encode;
    }

    /**
     * 每读取到一行数据，添加到缓存中
     *
     * @param line 读取到的数据
     * @param lineNo 行号
     * @param isEnd 是否读取完成
     * @throws Exception
     */
    public abstract void output(String line, long lineNo, boolean isEnd) throws Exception;
}