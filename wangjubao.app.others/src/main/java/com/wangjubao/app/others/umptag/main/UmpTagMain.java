package com.wangjubao.app.others.umptag.main;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.biz.service.job.AppContext;

//121.43.197.181
public class UmpTagMain {

	static Logger logger = Logger.getLogger("umptag");
	
	public static void main(String[] args) {

        long start = System.currentTimeMillis();
        System.out.println(" ####################################### start ... "+start);
        logger.info(String.format(" UmpTagMain main start (%s)", new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss:sss").format(new Date())));
        AppContext.setContext(new String[] { "classpath*:/META-INF/spring/*.xml",
                "classpath*:/spring/*.xml" });
        SynContext.setContext(AppContext.getContext());
        logger.info("---------------------启动优惠标签同步主线程");
        UmpTagSche umpTagSche = AppContext.getContext().getBean(UmpTagSche.class);
        umpTagSche.initSeller();
        umpTagSche.check();
        
        logger.info("---------------------订单优惠标签同步主线程启动 成功"); 
        
    
	}
}
