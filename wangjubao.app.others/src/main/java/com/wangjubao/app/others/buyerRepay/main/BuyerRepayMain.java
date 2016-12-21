package com.wangjubao.app.others.buyerRepay.main;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import com.wangjubao.app.others.consume.ConsumeCalculate;
import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.biz.service.job.AppContext;

/**
 * 计算商品回购周期
 * @author luorixin
 *
 */
public class BuyerRepayMain {

    public static final Logger     logger = Logger.getLogger(BuyerRepayMain.class);
    
    public static BuyerRepayReport buyerRepayReport;
    
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        System.out.println(" ####################################### start ... ");
        logger.info(String.format(" BuyerRepayMain main start (%s)", new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss:sss").format(new Date())));
        AppContext.setContext(new String[] { "classpath*:/META-INF/spring/*.xml",
                "classpath*:/spring/*.xml" });
        SynContext.setContext(AppContext.getContext());
        logger.info("---------------------启动 买家回购分析计算主线程");
        buyerRepayReport = new BuyerRepayReport();
        buyerRepayReport.doDataFetch();
        logger.info("---------------------买家回购分析计算主线程启动 成功");
        
    }
}
