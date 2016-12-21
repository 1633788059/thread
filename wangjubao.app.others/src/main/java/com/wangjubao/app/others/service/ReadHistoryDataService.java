package com.wangjubao.app.others.service;

import java.util.ArrayList;
import java.util.List;

import com.wangjubao.app.others.service.impl.extend.DataImportDo;

/**
 * @author ckex created 2013-5-29 - 上午11:09:21 ImportHistoryDataService.java
 * @explain -
 */
public interface ReadHistoryDataService {

    /**
     * 
     */
    void init();

    /**
     * 
     */
    void execute();

    /**
     * 读取文件放入指定队列
     * 
     * @param filePath
     * @param charset
     * @return 读完的文件名
     */
    public DataImportDo offerData(DataImportDo dataImportDo);

    /**
     * 消耗指定队列
     */
    public String[] pollDate(int k);

    /**
     * 写入db
     */
    public Long saveData(Long sellerId);

    public void executeHistoryDataImport(Long sellerId);

    /**
     * 读取csv文件，
     * 
     * @param filePath 文件路径
     * @param querySize 返回队列大小
     * @param charset 字符集
     * @return
     */
    List<ArrayList<String[]>> readCsvFile(String filePath, String charset, Integer querySize)
            throws Exception;

    /**
     * 读取csv文件，
     * 
     * @param filePath 文件路径
     * @param querySize 返回队列大小
     * @param charset 字符集 默認utf-8
     * @return
     */
    //    List<ArrayList<String[]>> readCsvFile(String filePath, Integer querySize) throws Exception;

    /**
     * 读取csv文件，
     * 
     * @param filePath 文件路径
     * @param charset 字符集 默認utf-8
     * @return
     */
    //    ArrayList<String[]> readCsvFile(String filePath) throws Exception;

    /**
     * @param sellerId
     * @param trade
     * @param order
     * @return
     */
    //    boolean insertTrade(Long sellerId, TradeDo trade, OrderDo order);

    /**
     * @param sellerId
     * @param values
     * @return
     */
    //    Integer insertTrades(Long sellerId, ArrayList<String[]> values);

    /**
     * 数据写入
     * 
     * @param sellerId
     * @param filePath 文件路径或文件夹路径，必须以csv
     * @param charset 字符集编码
     */
    //    void importHistoryData(Long sellerId, String filePath, String charset);

    /**
     * 数据写入
     * 
     * @param sellerId
     * @param filePath 采用默认路径 /home/deploy/orderdata/%sellerId%/
     * @param charset 字符集编码
     */
    //    void importHistoryData(Long sellerId, String charset);

    /**
     * 数据写入
     * 
     * @param sellerId
     * @param filePath 采用默认路径 /home/deploy/orderdata/%sellerId%/
     * @param charset 采用默认路径字符集编码 GBK
     */
    //    void importHistoryData(Long sellerId);

    /**
     * 检查历史数据的邮箱
     * 
     * @param sellerId
     */
    void synHistrotyEmail(Long sellerId);

    /**
     * 检查上一次任务未执行完而程序中断的导入任务
     */
    void recordData() throws Exception;

}
