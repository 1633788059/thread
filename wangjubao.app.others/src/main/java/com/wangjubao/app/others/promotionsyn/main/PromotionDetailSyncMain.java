package com.wangjubao.app.others.promotionsyn.main;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import com.wangjubao.app.others.sellercatssyn.main.SellercatsSche;
import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.biz.service.job.AppContext;
//121.41.172.56
public class PromotionDetailSyncMain {

	static Logger logger = Logger.getLogger("promotionsync");
    
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        System.out.println(" ####################################### start ... "+start);
        logger.info(String.format(" PromotionDetailSyncMain main start (%s)", new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss:sss").format(new Date())));
        AppContext.setContext(new String[] { "classpath*:/META-INF/spring/*.xml",
                "classpath*:/spring/*.xml" });
        SynContext.setContext(AppContext.getContext());
        logger.info("---------------------启动 订单优惠信息同步主线程");
        PromotionDetailSyncSche promotionSche = AppContext.getContext().getBean(PromotionDetailSyncSche.class);
        promotionSche.initSeller();
        promotionSche.check();
        
        logger.info("---------------------订单优惠信息同步主线程启动 成功"); 
        
    }
}
