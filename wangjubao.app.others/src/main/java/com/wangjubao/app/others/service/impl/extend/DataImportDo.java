package com.wangjubao.app.others.service.impl.extend;

/**
 * @author ckex created 2013-8-13 - 上午11:54:08 HistoryDataImportDo.java
 * @explain -
 */
public class DataImportDo {

    private String  path;
    private String  charset;
    private Long    sellerId;
    private Integer readNum;
    private Integer insertNum;
    private Long    length;
    private Long    readTime;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Long getSellerId() {
        return sellerId;
    }

    public void setSellerId(Long sellerId) {
        this.sellerId = sellerId;
    }

    public Integer getReadNum() {
        return readNum;
    }

    public void setReadNum(Integer readNum) {
        this.readNum = readNum;
    }

    public Integer getInsertNum() {
        return insertNum;
    }

    public void setInsertNum(Integer insertNum) {
        this.insertNum = insertNum;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public Long getLength() {
        return length;
    }

    public void setLength(Long length) {
        this.length = length;
    }

    public Long getReadTime() {
        return readTime;
    }

    public void setReadTime(Long readTime) {
        this.readTime = readTime;
    }

    @Override
    public String toString() {
        return "DataImportDo [path=" + path + ", charset=" + charset + ", sellerId=" + sellerId
                + ", readNum=" + readNum + ", insertNum=" + insertNum + ", length=" + length
                + ", readTime=" + readTime + "-ms]";
    }

}
