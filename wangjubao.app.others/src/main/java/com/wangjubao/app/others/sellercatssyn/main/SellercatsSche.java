package com.wangjubao.app.others.sellercatssyn.main;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.app.others.service.ConfigureThreadPoolTask;
import com.wangjubao.app.others.service.SellerInfoUpdateTask;
import com.wangjubao.dolphin.common.util.date.DateUtils;

@Service
public class SellercatsSche {

	  static Logger logger = Logger.getLogger("sellercats");
	
	@Autowired
	private ConfigureThreadPoolTask configureThreadPoolTask;
	
	@Autowired
	private SellerInfoUpdateTask sellerInfoUpdateTask;
	
	public void initSeller() {
		logger.info(" ####################################### 卖家信息分类启动..." + DateUtils.formatDate(DateUtils.now()));
		ScheduledThreadPoolExecutor sellerInfoInit = new ScheduledThreadPoolExecutor(1);
		sellerInfoInit.scheduleWithFixedDelay(sellerInfoUpdateTask,  0, 1, TimeUnit.MINUTES);
	}
	
	public void check() {
		logger.info(" ####################################### 卖家分类更新启动..." + DateUtils.formatDate(DateUtils.now()));
		ScheduledThreadPoolExecutor sellerInfoInit = new ScheduledThreadPoolExecutor(1);
		sellerInfoInit.scheduleWithFixedDelay(configureThreadPoolTask,  0, 1, TimeUnit.HOURS);
	}
}
