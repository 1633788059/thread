package com.wangjubao.app.others.service.promotionsync;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.dolphin.common.util.StrUtils;

@Service
public class PromotionThreadPoolTask implements Runnable{

	 static Logger logger = Logger.getLogger("promotionsync");
	@Autowired
	private PromotionThreadPoolService configureThreadPoolService;
	
	@Override
	public void run() {
  	try {
  		configureThreadPoolService.startTaskInThreadPool();
		} catch (Exception e) {
			if (logger.isInfoEnabled())
	        {
	        	logger.warn(StrUtils.showError(e));
	        }
		}
  }

}
