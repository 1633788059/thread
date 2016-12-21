package com.wangjubao.app.others.authcheck.main;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.app.others.service.SellerInfoMap;
import com.wangjubao.app.others.service.SellerInfoUpdateTask;
import com.wangjubao.app.others.service.authcheck.AuthCheckTask;
import com.wangjubao.dolphin.common.util.date.DateUtils;


@Service
public class AuthCheckSche {

	  static Logger logger = Logger.getLogger("authcheck");
	
	@Autowired
	private SellerInfoMap sellerInfoMap;
	
	@Autowired
	private AuthCheckTask authCheckTask;
	
	@Autowired
	private SellerInfoUpdateTask sellerInfoUpdateTask;
	
	public void initSeller() {
		logger.info(" ####################################### 卖家信息分类启动..." + DateUtils.formatDate(DateUtils.now()));
		ScheduledThreadPoolExecutor sellerInfoInit = new ScheduledThreadPoolExecutor(1);
		sellerInfoInit.scheduleWithFixedDelay(sellerInfoUpdateTask,  0, 6, TimeUnit.HOURS);
	}
	
	public void check() {
		logger.info(" ####################################### 卖家授权检查启动..." + DateUtils.formatDate(DateUtils.now()));
		ScheduledThreadPoolExecutor sellerInfoInit = new ScheduledThreadPoolExecutor(1);
		sellerInfoInit.scheduleWithFixedDelay(authCheckTask,  0, 6, TimeUnit.HOURS);
	}
}

