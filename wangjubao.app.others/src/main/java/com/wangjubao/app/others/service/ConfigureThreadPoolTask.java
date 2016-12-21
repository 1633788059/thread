package com.wangjubao.app.others.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.dolphin.common.util.StrUtils;


@Service
public class ConfigureThreadPoolTask implements Runnable{

	  protected transient final static Logger   logger                               = LoggerFactory
	            .getLogger(ConfigureThreadPoolTask.class);
	@Autowired
	private ConfigureThreadPoolService configureThreadPoolService;
	
	@Override
	public void run() {
    	try {
    		configureThreadPoolService.startTaskInThreadPool();
		} catch (Exception e) {
			if (logger.isWarnEnabled())
	        {
	        	logger.warn(StrUtils.showError(e));
	        }
		}
    }

}
