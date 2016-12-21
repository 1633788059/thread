package com.wangjubao.app.others.authcheck.main;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import com.wangjubao.app.others.service.authcheck.AuthCheckService;
import com.wangjubao.app.others.service.itemCatSync.ItemCatSyncService;
import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.biz.service.job.AppContext;

public class AuthCheckMain {

    static final Logger logger = Logger.getLogger("authcheck");

    public static void main(String[] args)
    {
        long start = System.currentTimeMillis();
        System.out.println(" ####################################### start ... " + start);
        logger.info(String.format(" AuthCheckMain main start (%s)", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sss").format(new Date())));
        AppContext.setContext(new String[] { "classpath*:/META-INF/spring/*.xml", "classpath*:/spring/*.xml" });
        SynContext.setContext(AppContext.getContext());
        logger.info("---------------------启动 授权同步主线程");
        AuthCheckSche authCheckSche = AppContext.getContext().getBean(AuthCheckSche.class);

        authCheckSche.initSeller();
        authCheckSche.check();
        logger.info("---------------------授权同步主线程启动 成功");

    }
}
