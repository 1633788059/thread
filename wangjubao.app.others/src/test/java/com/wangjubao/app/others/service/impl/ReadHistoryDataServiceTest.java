package com.wangjubao.app.others.service.impl;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.wangjubao.app.others.service.AbstractWangjubaoTest;
import com.wangjubao.app.others.service.impl.extend.DataImportDo;

/**
 * @author ckex created 2013-8-7 - 下午12:51:01 ReadHistoryDataServiceTest.java
 * @explain -
 */
public class ReadHistoryDataServiceTest extends AbstractWangjubaoTest {

    ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 5, 100, TimeUnit.MILLISECONDS,
                                        new LinkedBlockingQueue<Runnable>(),
                                        new ThreadPoolExecutor.AbortPolicy());

    @Test
    public void testExecute() {
        readHistoryDataService.execute();
        readHistoryDataService.executeHistoryDataImport(291884081l);
        synchronized (ReadHistoryDataServiceTest.class) {
            while (true) {
                try {
                    ReadHistoryDataServiceTest.class.wait();
                } catch (Throwable e) {
                }
            }
        }
    }

    //    @Test
    public void testDeque() {
        long start = System.currentTimeMillis();

        readHistoryDataService.execute();

        readHistoryDataService.executeHistoryDataImport(291884081l);

        new Thread() {

            @Override
            public void run() {
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                    while (true) {
                        TimeUnit.MICROSECONDS.sleep(100);
                        String[] v = readHistoryDataService.pollDate(15);
                        if (v == null) {
                            System.out.println(" is null sleep . ");
                            TimeUnit.SECONDS.sleep(1);
                        } else {
                            //                            System.out.println(Arrays.toString(v));
                        }

                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        logger.info(String.format(" start . %s ", start));

        final DataImportDo dataImportDo = new DataImportDo();
        dataImportDo.setPath("D:/orderdata/291884081/ExportOrderList201308061438.csv");
        dataImportDo.setSellerId(291884081l);
        dataImportDo.setCharset("GBK");

        Future<DataImportDo> path = executor.submit(new Callable<DataImportDo>() {

            @Override
            public DataImportDo call() throws Exception {
                return readHistoryDataService.offerData(dataImportDo);
            }

        });

        while (!path.isDone()) {
            Thread.yield();
        }

        try {
            System.out.println(path.get().toString());
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        } catch (ExecutionException e1) {
            e1.printStackTrace();
        }

        synchronized (ReadHistoryDataServiceTest.class) {
            while (true) {
                try {
                    ReadHistoryDataServiceTest.class.wait();
                } catch (Throwable e) {
                }
            }
        }
    }

}
