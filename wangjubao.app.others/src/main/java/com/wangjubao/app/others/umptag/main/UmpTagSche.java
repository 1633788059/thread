package com.wangjubao.app.others.umptag.main;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.app.others.service.SellerInfoUpdateTask;
import com.wangjubao.app.others.service.umptag.UmpTagThreadPoolTask;
import com.wangjubao.dolphin.common.util.date.DateUtils;

@Service
public class UmpTagSche {


	  static Logger logger = Logger.getLogger("umptag");
	
	@Autowired
	private UmpTagThreadPoolTask configureThreadPoolTask;
	
	@Autowired
	private SellerInfoUpdateTask sellerInfoUpdateTask;
	
	
	public void initSeller() {
		/*logger.info(" ####################################### 卖家信息分类启动..." + DateUtils.formatDate(DateUtils.now()));
		ScheduledThreadPoolExecutor sellerInfoInit = new ScheduledThreadPoolExecutor(1);
		sellerInfoInit.scheduleWithFixedDelay(sellerInfoUpdateTask,  0, 1, TimeUnit.MINUTES);*/
	}
	
	public void check() {
		logger.info(" ####################################### ump打标签启动..." + DateUtils.formatDate(DateUtils.now()));
		ScheduledThreadPoolExecutor sellerInfoInit = new ScheduledThreadPoolExecutor(1);
		sellerInfoInit.scheduleWithFixedDelay(configureThreadPoolTask,  2, 60*24, TimeUnit.MINUTES);
	}
	

}
