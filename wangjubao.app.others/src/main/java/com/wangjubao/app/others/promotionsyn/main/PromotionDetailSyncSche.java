package com.wangjubao.app.others.promotionsyn.main;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.app.others.service.SellerInfoUpdateTask;
import com.wangjubao.app.others.service.promotionsync.PromotionThreadPoolTask;
import com.wangjubao.dolphin.common.util.date.DateUtils;

@Service
public class PromotionDetailSyncSche {

	  static Logger logger = Logger.getLogger("promotionsync");
	
	@Autowired
	private PromotionThreadPoolTask configureThreadPoolTask;
	
	@Autowired
	private SellerInfoUpdateTask sellerInfoUpdateTask;
	
	public void initSeller() {
		logger.info(" ####################################### 卖家信息分类启动..." + DateUtils.formatDate(DateUtils.now()));
		ScheduledThreadPoolExecutor sellerInfoInit = new ScheduledThreadPoolExecutor(1);
		sellerInfoInit.scheduleWithFixedDelay(sellerInfoUpdateTask,  10, 10, TimeUnit.MINUTES);
	}
	
	public void check() {
		logger.info(" ####################################### 订单优惠信息更新启动..." + DateUtils.formatDate(DateUtils.now()));
		ScheduledThreadPoolExecutor sellerInfoInit = new ScheduledThreadPoolExecutor(1);
		sellerInfoInit.scheduleWithFixedDelay(configureThreadPoolTask,  0, 10, TimeUnit.MINUTES);
	}
}
