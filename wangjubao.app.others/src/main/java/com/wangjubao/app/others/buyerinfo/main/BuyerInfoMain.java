package com.wangjubao.app.others.buyerinfo.main;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wangjubao.app.others.service.BuyerInfoService;
import com.wangjubao.app.others.service.impl.TraderatesServiceImpl;
import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.biz.service.job.AppContext;

/**
 * @author ckex created 2013-6-24 - 下午2:13:18 BuyerInfoMain.java
 * @explain - 同步buyer信息 main 暂放113机器
 */
public class BuyerInfoMain {

    public transient final static Logger logger = LoggerFactory.getLogger("buyer-info");

    public static void main(String[] args)
    {

        long start = System.currentTimeMillis();
        System.out.println(" ################## BuyerInfoMain start ... ");
        logger.info(String.format(" BuyerInfoMain start (%s)", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sss").format(new Date())));
        AppContext.setContext(new String[] { "classpath*:/META-INF/spring/*.xml", "classpath*:/spring/*.xml" });
        SynContext.setContext(AppContext.getContext());

        BuyerInfoService buyerInfoService = AppContext.getContext().getBean(BuyerInfoService.class);
        buyerInfoService.syncByuerInfo();

        logger.info(String.format(" BuyerInfoMain started times :  (%s) -ms ", (System.currentTimeMillis() - start)));

        /******************************************************************/
        // 拉取评价的,在219服务器上启动
        //        logger.info("---------------------评价拉取线程启动...");
        //        TraderatesServiceImpl traderatesService = new TraderatesServiceImpl();
        //        traderatesService.execute();
        /******************************************************************/

        System.out.println(" ################## BuyerInfoMain started. ");
    }

}
