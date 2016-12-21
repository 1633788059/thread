package com.wangjubao.app.others.downcenter.main;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import com.wangjubao.app.others.promotionsyn.main.PromotionDetailSyncSche;
import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.biz.service.job.AppContext;

//121.43.197.186
public class DownCenterMain {

	static Logger logger = Logger.getLogger("downcenter");
    
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        System.out.println(" ####################################### start ... "+start);
        logger.info(String.format(" DownCenterMain main start (%s)", new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss:sss").format(new Date())));
        AppContext.setContext(new String[] { "classpath*:/META-INF/spring/*.xml",
                "classpath*:/spring/*.xml" });
        SynContext.setContext(AppContext.getContext());
        logger.info("---------------------启动 下载上传主线程");
        DownCenterSche downCenterSche = AppContext.getContext().getBean(DownCenterSche.class);
        downCenterSche.startJob();
        downCenterSche.cleanJob();
        logger.info("---------------------下载上传主线程启动 成功"); 
        
    }
}
