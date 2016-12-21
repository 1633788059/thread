package com.wangjubao.app.others.historydata.main;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import com.wangjubao.app.others.buyerRepay.main.BuyerRepayReport;
import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.biz.common.standalone.StandaloneApp;
import com.wangjubao.dolphin.biz.service.job.AppContext;

/**
 * @author ckex created 2013-6-25 - 上午11:06:55 HistoryDataMain.java
 * @explain - 历史数据导入 main 与web同机器 48
 */
public class HistoryDataMain {

    public static final Logger logger = Logger.getLogger(HistoryDataMain.class);
    
    public static BuyerRepayReport buyerRepayReport;

    public static void main(String[] args) {

        long start = System.currentTimeMillis();
        System.out.println("################## HistoryDataMain start ... ");
        logger.info(String.format(" other main start (%s)", new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss:sss").format(new Date())));
        AppContext.setContext(new String[] { "classpath*:/META-INF/spring/*.xml",
                "classpath*:/spring/*.xml" });
        SynContext.setContext(AppContext.getContext());

        StandaloneApp standaloneApp = (StandaloneApp) AppContext.getBean("standaloneApp");
        standaloneApp.execute();
        
        /////////////////////////////////////////////////////////
//        logger.info("---------------------启动 买家回购分析计算主线程");
//        buyerRepayReport = new BuyerRepayReport();
//        buyerRepayReport.doDataFetch();
//        logger.info("---------------------买家回购分析计算主线程启动 成功");
        ////////////////////////////////////////////////////////

        logger.info(String.format(" other main started times :  (%s) -ms ",
                (System.currentTimeMillis() - start)));

        synchronized (HistoryDataMain.class) {
            while (true) {
                try {
                    HistoryDataMain.class.wait();
                } catch (Throwable e) {
                }
            }
        }

    }
}
