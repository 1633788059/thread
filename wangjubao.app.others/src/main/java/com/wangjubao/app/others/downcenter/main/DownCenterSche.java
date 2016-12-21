package com.wangjubao.app.others.downcenter.main;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.app.others.service.downcenter.DownCenterCleanTask;
import com.wangjubao.app.others.service.downcenter.DownCenterTask;
import com.wangjubao.dolphin.common.util.date.DateUtils;


@Service
public class DownCenterSche {
	static Logger logger = Logger.getLogger("downcenter");

	@Autowired
	private DownCenterTask downCenterTask;
	
	@Autowired
	private DownCenterCleanTask downCenterCleanTask;
	
	public void startJob() {
		logger.info(" ####################################### 下载中心下载任务启动..." + DateUtils.formatDate(DateUtils.now()));
		//初始化
		downCenterTask.initDoing();
		logger.info(" ####################################### 下载中心下载任务初始化成功.." + DateUtils.formatDate(DateUtils.now()));
		ScheduledThreadPoolExecutor sellerInfoInit = new ScheduledThreadPoolExecutor(1);
		sellerInfoInit.scheduleWithFixedDelay(downCenterTask,  0, 30, TimeUnit.SECONDS);
	}

	public void cleanJob() {
		logger.info(" ####################################### 下载中心清除过期任务启动..." + DateUtils.formatDate(DateUtils.now()));
		ScheduledThreadPoolExecutor sellerInfoInit = new ScheduledThreadPoolExecutor(1);
		sellerInfoInit.scheduleWithFixedDelay(downCenterCleanTask,  0, 1, TimeUnit.DAYS);
	}
	
	
}
