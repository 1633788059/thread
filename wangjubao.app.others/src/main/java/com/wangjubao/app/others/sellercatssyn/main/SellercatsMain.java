package com.wangjubao.app.others.sellercatssyn.main;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.biz.service.job.AppContext;

public class SellercatsMain {

	static final Logger                               logger       = Logger.getLogger("sellercatsMain");
    
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        System.out.println(" ####################################### start ... "+start);
        logger.info(String.format(" SellercatsMain main start (%s)", new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss:sss").format(new Date())));
        AppContext.setContext(new String[] { "classpath*:/META-INF/spring/*.xml",
                "classpath*:/spring/*.xml" });
        SynContext.setContext(AppContext.getContext());
        logger.info("---------------------启动 类目同步主线程");
        SellercatsSche sellercatsSche = AppContext.getContext().getBean(SellercatsSche.class);
        sellercatsSche.initSeller();
        sellercatsSche.check();
        
        logger.info("---------------------类目同步主线程启动 成功"); 
        
    }
}
