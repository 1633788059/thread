package com.wangjubao.app.others.historydata.main;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.quartz.Scheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.app.others.service.BuyerInfoService;
import com.wangjubao.app.others.service.OthreService;
import com.wangjubao.app.others.service.ReadHistoryDataService;
import com.wangjubao.dolphin.biz.common.standalone.StandaloneApp;
import com.wangjubao.dolphin.biz.service.job.DolphinJobListener;
import com.wangjubao.dolphin.biz.service.job.DolphinSchedulerListener;
import com.wangjubao.dolphin.biz.service.job.JobOtherQuartz;

/**
 * @author ckex created 2013-6-15 下午2:16:03
 * @explain -
 */
@Service("standaloneApp")
public class HistoryDataImportApp implements StandaloneApp {

    private transient final static Logger logger = Logger.getLogger("histroyimport");

    @Autowired
    private DolphinJobListener            dolphinJobListener;

    @Autowired
    private DolphinSchedulerListener      dolphinSchedulerListener;

    @Autowired
    private ReadHistoryDataService        readHistoryDataService;

    @Autowired
    private BuyerInfoService              buyerInfoService;

    @Autowired
    private OthreService                  othreService;

    public void setDolphinJobListener(DolphinJobListener dolphinJobListener) {
        this.dolphinJobListener = dolphinJobListener;
    }

    public void setDolphinSchedulerListener(DolphinSchedulerListener dolphinSchedulerListener) {
        this.dolphinSchedulerListener = dolphinSchedulerListener;
    }

    @PostConstruct
    @Override
    public void init() {
        // TODO Auto-generated method stub
    }

    @Override
    public void execute() {
        if (logger.isInfoEnabled()) {
            logger.info("execute in HistoryDataImportApp.");
        }
        try {
            long start = System.currentTimeMillis();
            logger.info(String.format(" job other scheduler start (%s)", new SimpleDateFormat(
                    "yyyy-MM-dd HH:mm:ss:sss").format(new Date())));
            logger.info("history data import start . ");
            Scheduler s = null; // JobOtherQuartz.getInstance().getScheduler();
            s.getListenerManager().addJobListener(dolphinJobListener);
            s.getListenerManager().addSchedulerListener(dolphinSchedulerListener);
            //            s.start();
            logger.info(String.format("  job other scheduler started  times :  (%s) -ms ",
                    (System.currentTimeMillis() - start)));
        } catch (Exception ex) {
            logger.debug(ex.getMessage(), ex);
        }

        //        recordHistoryData();

        //        readHistoryDataService.execute();

        /****************************************************************************/
        new Thread() {
            // 臨時代碼 。
            /**
             * 356581685l shes饰品旗舰店 175473761
             */
            @Override
            public void run() {

                // 重新获取一次买家email
                //                readHistoryDataService.synHistrotyEmail(23849921l);

                // 重新获取一次会员地址信息
                /*
                 * 268466435 诺奇旗舰店 87444387 ihaveflyingwing
                 */
                Long[] sellerids = new Long[] { 268466435l, 87444387l };
                for (Long sellerId : sellerids) {
                    //                    othreService.recountBuyerAddress(sellerId);
                }
            }
        }.start();

        // buyer信息 buyerinfoMain
        /****************************************************************************/
        //        new Thread() {
        //
        //            @Override
        //            public void run() {
        //                Timer timer = new Timer();
        //                TimerTask task = new TimerTask() {
        //                    public void run() {
        //                        Calendar c = Calendar.getInstance();
        //                        int hours = c.get(Calendar.HOUR_OF_DAY);
        //                        if (hours == 33) {
        //                            buyerInfoService.execute();
        //                        }
        //                    }
        //                };
        //                timer.scheduleAtFixedRate(task, 1000, 1000 * 60 * 40);
        //            }
        //        }.start();

    }

    private void recordHistoryData() {
        try {
            readHistoryDataService.recordData();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(" record erro : " + Arrays.toString(e.getStackTrace()));
        }
    }

}
