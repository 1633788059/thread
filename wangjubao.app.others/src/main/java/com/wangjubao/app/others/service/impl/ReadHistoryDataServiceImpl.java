package com.wangjubao.app.others.service.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.csvreader.CsvReader;
import com.wangjubao.app.others.service.ReadHistoryDataService;
import com.wangjubao.app.others.service.impl.extend.DataImportDo;
import com.wangjubao.dolphin.biz.common.constant.BaseType;
import com.wangjubao.dolphin.biz.dal.extend.bo.ProductBo;
import com.wangjubao.dolphin.biz.dao.HistoryDataImportDao;
import com.wangjubao.dolphin.biz.model.HistoryDataImportDo;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.biz.service.AuthoriztionService;
import com.wangjubao.dolphin.biz.service.JobOtherService;
import com.wangjubao.dolphin.common.util.StrUtils;
import com.wangjubao.dolphin.common.util.date.DateUtils;
import com.wangjubao.dolphin.standalone.datap.service.support.ConverCsv2TradeDo;
import com.wangjubao.dolphin.standalone.datap.service.support.HandelDataSupport;

/**
 * @author ckex created 2013-5-29 - 上午11:10:03 ImportHistoryDataServiceImpl.java
 * @explain -历史数据导入
 */
@Service("readHistoryDataService")
public class ReadHistoryDataServiceImpl implements ReadHistoryDataService {

    private transient final static Logger            logger                      = LoggerFactory
                                                                                         .getLogger("histroyimport");

    @Autowired
    private HistoryDataImportDao                     historyDataImportDao;

    @Autowired
    private HandelDataSupport                        handelDataSupport;

    @Autowired
    private AuthoriztionService                      authoriztionService;

    @Autowired
    private JobOtherService                          jobOtherService;

    @Autowired
    private ProductBo                                productBo;

    private Random                                   rnd                         = new Random();

    private static final Map<String, AtomicBoolean>  READ_STATUS                 = new ConcurrentHashMap<String, AtomicBoolean>();

    private static LinkedBlockingDeque<String[]>[]   dequeGroup                  = new LinkedBlockingDeque[5];

    private static int                               MAX_SIZE                    = 1;

    private static final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(
                                                                                         3);

    private static final ThreadPoolExecutor          executor                    = new ThreadPoolExecutor(
                                                                                         MAX_SIZE,
                                                                                         MAX_SIZE,
                                                                                         100,
                                                                                         TimeUnit.MILLISECONDS,
                                                                                         new LinkedBlockingQueue<Runnable>(),
                                                                                         new ThreadPoolExecutor.AbortPolicy());   // 异常处理机制 拒绝任务并抛出异常

    private final ExecutorService                    thread                      = Executors
                                                                                         .newSingleThreadExecutor();

    private String                                   defaultPath;

    public void setHistoryDataImportDao(HistoryDataImportDao historyDataImportDao) {
        this.historyDataImportDao = historyDataImportDao;
    }

    public void setHandelDataSupport(HandelDataSupport handelDataSupport) {
        this.handelDataSupport = handelDataSupport;
    }

    @Value("#{settings['defaultPath']}")
    public void setDefaultPath(String defaultPath) {
        this.defaultPath = defaultPath;
    }

    @PostConstruct
    @Override
    public void init() {
        logger.info(String.format(" initialize default path : (%s)", defaultPath));
        for (int i = 0; i < dequeGroup.length; i++) {
            dequeGroup[i] = new LinkedBlockingDeque<String[]>(2000);
        }
    }

    @Override
    public void execute() {

        scheduledThreadPoolExecutor.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                try {
                    logger.info(" start 周期调度。 ^^^^^^^^^^^^^^^^^^^^^^^^^^");
                    addTask(0l);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.equals(" XXXXXXXXXXXXX" + Arrays.toString(e.getStackTrace()));
                }
            }
        }, 1, 60 * 3, TimeUnit.SECONDS);

    }

    @Override
    public void executeHistoryDataImport(final Long sellerId) {

        String filePath = defaultPath + File.separator + sellerId.toString();
        if (logger.isInfoEnabled()) {
            logger.info(sellerId + " begin execute history data import . " + filePath);
        }
        if (!READ_STATUS.containsKey(sellerId.toString())) {
            READ_STATUS.put(sellerId.toString(), new AtomicBoolean(false));
        }
        addTask(sellerId);
        try {
            executeHistoryDataImport(sellerId, filePath, "GBK");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(" XXXX " + Arrays.toString(e.getStackTrace()));
        } finally {
            READ_STATUS.get(sellerId.toString()).set(true);
        }

        if (logger.isInfoEnabled()) {
            logger.info(sellerId + " execute history data import over ~_*  " + filePath);
        }
    }

    private void addTask(final Long sellerId) {

        scheduledThreadPoolExecutor.schedule(new Runnable() {

            @Override
            public void run() {

                Future<Long> count = executor.submit(new Callable<Long>() {

                    @Override
                    public Long call() throws Exception {
                        logger.info(sellerId
                                + " \u65b0\u5efa\u4efb\u52d9\uff0c \u6570\u636e\u6b63\u5728\u5165\u4e2d ...");
                        for (int i = 0; i < dequeGroup.length; i++) {
                            logger.info("index " + i + " deque size : " + dequeGroup[i].size());
                        }
                        int k = 0;
                        while (sellerId.longValue() != 0
                                && !READ_STATUS.get(sellerId.toString()).get()) {
                            for (int i = 0; i < dequeGroup.length; i++) {
                                logger.info("index " + i + " deque size : " + dequeGroup[i].size());
                            }
                            logger.info(" data is empty . I will go to sleep " + 10 + " seconds . ");
                            TimeUnit.SECONDS.sleep(10);
                            if (++k > 13) {
                                break;
                            }
                        }
                        return saveData(sellerId);
                    }

                });

                try {
                    while (!count.isDone()) {
                        TimeUnit.MILLISECONDS.sleep(1000 * 5);
                        Thread.yield();
                    }
                    Long num = count.get();
                    logger.info(sellerId
                            + " ## \u4e00\u6279\u6570\u636e\u5bfc\u5165\u5b8c\u6210\u3002\u5171 : "
                            + num + "  \u6761\u8bb0\u5f55.");
                    boolean flag = Boolean.FALSE;
                    for (int i = 0; i < dequeGroup.length; i++) {
                        int size = dequeGroup[i].size();
                        logger.info("index " + i + " deque size : " + size);
                        if (size > 0) {
                            flag = Boolean.TRUE;
                        }
                    }
                    if (flag) {
                        addTask(sellerId);
                    }
                    logger.info(" started ^^^^^ " + DateUtils.formatDate(DateUtils.now()));
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.equals(" ^^^^^^^^^^^^^^^^^ " + Arrays.toString(e.getStackTrace()));
                }
            }
        }, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public Long saveData(Long sellerId) {
        Long start = null;
        ConverCsv2TradeDo conver = new ConverCsv2TradeDo();
        int k = rnd.nextInt(50);
        String[] data = pollDate(k);
        AtomicLong counter = new AtomicLong(0);
        while (data != null) {
            start = System.currentTimeMillis();
            if (insertTrades(data, conver)) {
                counter.getAndIncrement();
                if (logger.isInfoEnabled()) {
                    logger.info(String.format("{ %s } import trade . size(%s) times (%s)-ms",
                            sellerId, counter.get(), (System.currentTimeMillis() - start)));
                }
            } else {
                logger.info(sellerId + " import error : " + Arrays.toString(data));
            }
            if (k >= Integer.MAX_VALUE) {
                k = 10;
            }
            data = pollDate(++k);
        }
        logger.info(String.format("{ %s } queue is empty sleep . 【%s】", sellerId,
                DateUtils.formatDate(DateUtils.now())));
        return counter.get();
    }

    private boolean insertTrades(String[] value, ConverCsv2TradeDo conver) {
        try {
            return conver.ConverCsv(null, value, handelDataSupport);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(" XXXXXXXXXXXXX " + Arrays.toString(e.getStackTrace()));
            return Boolean.FALSE;
        }
    }

    @Override
    public DataImportDo offerData(DataImportDo dataimportDo) {
        long start = System.currentTimeMillis();
        CsvReader reader = null;
        try {
            File file = new File(dataimportDo.getPath());
            if (!file.exists()) {
                file.mkdir();
            }
            if (!file.exists()) {
                logger.error(" Is not a valid path to the file. checkd : " + file.getAbsolutePath());
                throw new IllegalArgumentException(" Is not a valid path to the file. checkd : "
                        + file.getAbsolutePath());
            }
            if (!StrUtils.isNotEmpty(dataimportDo.getCharset())) {
                dataimportDo.setCharset("UTF-8");
            }
            reader = new CsvReader(dataimportDo.getPath(), ',', Charset.forName(dataimportDo
                    .getCharset()));
            reader.readHeaders();
            int length = dequeGroup.length;
            int readCount = 0;
            while (reader.readRecord()) {
                int index = length - 1;
                String[] V = reader.getValues();
                if (V == null) {
                    continue;
                } else if (V.length < 27) {
                    logger.info(String
                            .format("\u4e0d\u5408\u6cd5\u6587\u4ef6\uff0c\u65e0\u6cd5\u5bfc\u5165\uff0c\u8bf7\u68c0\u67e5\u6587\u4ef6\u683c\u5f0f\u3002 【%s】",
                                    Arrays.toString(V)));
                    continue;
                }
                String nick = V[1];
                V[26] = dataimportDo.getSellerId().toString();
                if (StringUtils.isBlank(nick)) {
                    while (!dequeGroup[index].offerFirst(V, 20, TimeUnit.SECONDS)) {
                        logger.info(String.format(
                                "{ %s } queue too long, I will go to sleep 20 seconds .",
                                dataimportDo.getSellerId()));
                    }
                    ++readCount;
                } else {
                    index = nick.hashCode() % length;
                    if (index < 0) {
                        index = 0 - index;
                    }
                    while (!dequeGroup[index].offerFirst(V, 20, TimeUnit.SECONDS)) {
                        logger.info(String.format(
                                "{ %s } queue too long, I will go to sleep 20 seconds .",
                                dataimportDo.getSellerId()));
                    }
                    ++readCount;
                }
            }
            dataimportDo.setReadNum(readCount);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            reader.close();
        }
        dataimportDo.setReadTime(System.currentTimeMillis() - start);
        return dataimportDo;
    }

    @Override
    public String[] pollDate(int k) {
        int length = dequeGroup.length;
        String[] v = null;
        try {
            v = dequeGroup[k % length].pollLast(500, TimeUnit.MICROSECONDS);
            if (v != null) {
                return v;
            }
            for (int i = 0; i < length; i++) {
                v = dequeGroup[i].pollLast(500, TimeUnit.MICROSECONDS);
                if (v != null) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return v;
    }

    @Override
    public List<ArrayList<String[]>> readCsvFile(String filePath, String charset, Integer querySize)
            throws Exception {

        CsvReader reader = null;
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                file.mkdir();
            }
            if (!file.exists()) {
                logger.error(" Is not a valid path to the file. checkd : " + file.getAbsolutePath());
                throw new IllegalArgumentException(" Is not a valid path to the file. checkd : "
                        + file.getAbsolutePath());
            }
            if (querySize == null || querySize < 1) {
                querySize = 1;
            }
            ArrayList<ArrayList<String[]>> query = initQuery(querySize);
            if (StringUtils.isBlank(charset)) {
                charset = "UTF-8";
            }
            reader = new CsvReader(filePath, ',', Charset.forName(charset));
            reader.readHeaders();
            while (reader.readRecord()) {
                int index = (querySize - 1);
                String[] V = reader.getValues();
                if (V == null) {
                    continue;
                }
                String nick = null;
                if (V.length > 2) {
                    nick = V[1];
                }
                if (StringUtils.isBlank(nick)) {
                    query.get(index).add(reader.getValues());
                } else {
                    index = nick.hashCode() % querySize;
                    if (index < 0) {
                        index = 0 - index;
                    }
                    query.get(index).add(V);
                }
            }
            return query;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            reader.close();
        }
    }

    public void executeHistoryDataImport(Long sellerId, String filePath, String charset) {
        SellerDo sellerDo = productBo.getSellerDoById(sellerId);
        if (sellerDo == null) {
            throw new IllegalArgumentException(" seller can't be null. seller id  : " + sellerId);
        }
        if (StringUtils.isBlank(filePath)) {
            logger.error(" Is not a valid path to the file. checkd : " + filePath);
            throw new IllegalArgumentException(" Is not a valid path to the file. checkd : "
                    + filePath);
        }
        File file = new File(filePath);
        if (!file.exists()) {
            logger.error(" Is not a valid path to the file. checkd : " + file.getAbsolutePath());
            throw new IllegalArgumentException(" Is not a valid path to the file. checkd : "
                    + file.getAbsolutePath());
        }
        List<String> list = new ArrayList<String>();
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                File f = files[i];
                String fileName = f.getName();
                if (fileName.endsWith(".csv") || fileName.endsWith(".CSV")) {
                    String path = f.getAbsolutePath();
                    if (checkFileStatus(sellerId, fileName, path)) {
                        list.add(path);
                    }
                }
            }
        } else {
            String fileName = file.getName();
            if (fileName.endsWith(".csv") || fileName.endsWith(".CSV")) {
                if (checkFileStatus(sellerId, fileName, file.getAbsolutePath())) {
                    list.add(file.getAbsolutePath());
                }
            }
        }
        if (list.isEmpty()) {
            logger.info(" no file import . ");
            return;
        }
        int _num = 0;
        for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
            String path = iterator.next();
            long len = getLeng(path); // KB 
            final DataImportDo dataImportDo = new DataImportDo();
            dataImportDo.setPath(path);
            dataImportDo.setSellerId(sellerId);
            dataImportDo.setCharset(charset);
            dataImportDo.setLength(len);
            try {
                Future<DataImportDo> callPath = thread.submit(new Callable<DataImportDo>() {
                    @Override
                    public DataImportDo call() throws Exception {
                        READ_STATUS.get(dataImportDo.getSellerId().toString()).set(true);
                        return offerData(dataImportDo);
                    }

                });

                while (!callPath.isDone()) {
                    TimeUnit.MILLISECONDS.sleep(1000 * 5);
                    Thread.yield();
                }

                DataImportDo resultData = callPath.get();
                if (resultData != null) {
                    File fName = new File(path);
                    HistoryDataImportDo historyDataImportDo = new HistoryDataImportDo();
                    historyDataImportDo.setFileName(fName.getName());
                    historyDataImportDo.setSellerId(sellerId);
                    historyDataImportDo.setFilePath(path);
                    historyDataImportDo.setImportEndTime(System.currentTimeMillis());
                    historyDataImportDo.setImportRecord(resultData.getReadNum());
                    historyDataImportDo.setStatus(BaseType.FILE_IMPORT_OK);
                    historyDataImportDo.setVol(len);
                    updateFileStatus(sellerId, historyDataImportDo);
                    _num += resultData.getReadNum();
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error(" error message : " + e);
                File f = new File(path);
                HistoryDataImportDo errorDo = new HistoryDataImportDo();
                errorDo.setSellerId(sellerId);
                errorDo.setFileName(f.getName());
                errorDo.setStatus(BaseType.FILE_IMPORT_FAIL);
                String errorInfo = e.toString();
                if (errorInfo.length() > 999) {
                    errorDo.setFailInfo(errorInfo.substring(0, 888));
                } else {
                    errorDo.setFailInfo(errorInfo);
                }
                updateFileStatus(sellerId, errorDo);
            }
        }
        if (_num > 0) {
            recountBuyerDay(sellerId);
        }
    }

    /**
     * 触发中间表重算
     * 
     * @param sellerId
     */
    private void recountBuyerDay(Long sellerId) {
        authoriztionService.updateSellerDay(sellerId);
    }

    private void updateEmail(Long sellerId, ArrayList<String[]> values) {
        long start = System.currentTimeMillis();
        ConverCsv2TradeDo conver = new ConverCsv2TradeDo();
        logger.info(String.format(" ( %s )  insertTrades size 【%s】 beggin .(%s) ", sellerId,
                values.size(), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sss").format(new Date())));
        for (String[] v : values) {
            try {
                if (conver.updateHistoryEmail(sellerId, v, handelDataSupport)) {
                    // TODO
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.error(" insertTrades error ! sellerId : " + sellerId + " trade id : " + v[0]
                        + " Message : " + e);
                continue;
            }
        }
        logger.info(String.format("insertTrades success times : (%s) ",
                (System.currentTimeMillis() - start)));
    }

    private ArrayList<ArrayList<String[]>> initQuery(int size) {

        ArrayList<ArrayList<String[]>> list = new ArrayList<ArrayList<String[]>>();
        for (int i = 0; i < size; i++) {
            list.add(new ArrayList<String[]>());
        }
        return list;
    }

    private boolean checkFileStatus(Long sellerId, String fileName, String filePath) {

        if (sellerId == null || sellerId.longValue() < 1 || StringUtils.isBlank(fileName)) {
            return Boolean.FALSE;
        }
        List<HistoryDataImportDo> fileDo = historyDataImportDao.loadByfileName(sellerId, fileName,
                BaseType.COMPLETION_TYPE_TRADE);
        if (fileDo == null || fileDo.isEmpty()) {
            HistoryDataImportDo historyDataImportDo = new HistoryDataImportDo();
            historyDataImportDo.setSellerId(sellerId);
            historyDataImportDo.setFileName(fileName);
            historyDataImportDo.setFilePath(filePath);
            historyDataImportDo.setStatus(BaseType.FILE_IMPORT);
            historyDataImportDo.setImportBeginTime(System.currentTimeMillis());
            historyDataImportDao.create(historyDataImportDo);
            return Boolean.TRUE;
        }
        if (fileDo.size() > 1) {
            logger.error(sellerId + " 有重名文件. " + fileName);
            return Boolean.FALSE;
        }
        for (HistoryDataImportDo historyDataImportDo : fileDo) {
            Integer type = historyDataImportDo.getStatus();
            if (type == null || type.intValue() == BaseType.FILE_UPLOADING_OK) {
                historyDataImportDo.setSellerId(sellerId);
                historyDataImportDo.setFileName(fileName);
                historyDataImportDo.setFilePath(filePath);
                historyDataImportDo.setStatus(BaseType.FILE_IMPORT);
                historyDataImportDo.setImportBeginTime(System.currentTimeMillis());
                updateFileStatus(sellerId, historyDataImportDo);
                return Boolean.TRUE;
            } else if (type.intValue() == BaseType.FILE_IMPORT_FAIL) {
                historyDataImportDo.setSellerId(sellerId);
                historyDataImportDo.setFileName(fileName);
                historyDataImportDo.setFilePath(filePath);
                historyDataImportDo.setStatus(BaseType.FILE_IMPORT);
                historyDataImportDo.setFailInfo(null);
                historyDataImportDo.setImportBeginTime(System.currentTimeMillis());
                updateFileStatus(sellerId, historyDataImportDo);
                return Boolean.TRUE;
            }
        }
        return Boolean.FALSE;
    }

    private void updateFileStatus(Long sellerId, HistoryDataImportDo historyDataImportDo) {

        if (sellerId == null || historyDataImportDo == null) {
            throw new IllegalArgumentException(" update historyDataImportDo  must not be null . ");
        }
        List<HistoryDataImportDo> searchHistoryDataImportList = historyDataImportDao
                .loadByfileName(sellerId, historyDataImportDo.getFileName(),
                        BaseType.COMPLETION_TYPE_TRADE);
        if (searchHistoryDataImportList == null || searchHistoryDataImportList.isEmpty()) {
            historyDataImportDao.create(historyDataImportDo);
            return;
        } else {
            historyDataImportDo.setId(searchHistoryDataImportList.get(0).getId());
        }
        historyDataImportDo.setSellerId(sellerId);
        historyDataImportDao.update(historyDataImportDo);
    }

    private long getLeng(String path) {

        long l = 0;
        try {
            File f = new File(path);
            if (f.exists()) {
                FileInputStream fis = null;
                try {
                    fis = new FileInputStream(f);
                    l = (fis.available() / 1024);
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    if (fis != null) {
                        try {
                            fis.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(" 获取文件大小 error ！ " + e);
        }
        return l;
    }

    private void updateHistoryEmail(final Long sellerId, final ArrayList<String[]> arrayList) {

        executor.execute(new Runnable() {
            @Override
            public void run() {
                updateEmail(sellerId, arrayList);
            }
        });
    }

    /**
     * /home/deploy/orderdata
     */
    @Override
    public void synHistrotyEmail(Long sellerId) {
        String filePath = defaultPath + File.separator + sellerId.toString();
        File file = new File(filePath);
        if (!file.exists()) {
            logger.error(" Is not a valid path to the file. checkd : " + file.getAbsolutePath());
            throw new IllegalArgumentException(" Is not a valid path to the file. checkd : "
                    + file.getAbsolutePath());
        }
        List<String> list = new ArrayList<String>();
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (int i = 0; i < files.length; i++) {
                File f = files[i];
                String fileName = f.getName();
                if (fileName.endsWith(".csv") || fileName.endsWith(".CSV")) {
                    String path = f.getAbsolutePath();
                    list.add(path);
                }
            }
        } else {
            String fileName = file.getName();
            if (fileName.endsWith(".csv") || fileName.endsWith(".CSV")) {
                list.add(file.getAbsolutePath());
            }
        }

        if (list.isEmpty()) {
            logger.info(" no file import . ");
            return;
        }
        for (String path : list) {
            List<ArrayList<String[]>> trades = null;
            long len = getLeng(path); // KB 
            int querySize = 0;
            if (len < 10) {
                querySize = 2;
            } else if (len < 30) {
                querySize = 5;
            } else if (len < 60) {
                querySize = 15;
            } else if (len < 10000) {
                querySize = 300;
            } else {
                querySize = 1000;
            }
            try {
                trades = this.readCsvFile(path, "GBK", querySize);
            } catch (Exception e) {
                e.printStackTrace();
            }
            for (ArrayList<String[]> arrayList : trades) {
                updateHistoryEmail(sellerId, arrayList);
            }
        }
    }

    @Override
    public void recordData() throws Exception {
        List<HistoryDataImportDo> hisDo = historyDataImportDao
                .listImportFail(BaseType.COMPLETION_TYPE_TRADE);
        if (hisDo != null && !hisDo.isEmpty()) {
            for (HistoryDataImportDo historyDataImportDo : hisDo) {
                if (historyDataImportDo.getSellerId() != null) {
                    try {
                        jobOtherService.importHistoryData(historyDataImportDo.getSellerId(),
                                BaseType.COMPLETION_TYPE_TRADE);
                    } catch (Exception e) {
                        e.printStackTrace();
                        logger.error(String.format("【 %s 】Do : %s coredDate error : %s ",
                                historyDataImportDo.getSellerId(), historyDataImportDo.toString(),
                                Arrays.toString(e.getStackTrace())));
                        continue;
                    }
                }
            }
        }
    }

}
