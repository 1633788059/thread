package com.wangjubao.app.others.service.authcheck;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.dolphin.common.util.StrUtils;

@Service
public class AuthCheckTask implements Runnable {
	protected transient final static Logger logger = LoggerFactory
			.getLogger(AuthCheckTask.class);
	
	@Autowired
	private AuthCheckService authCheckService;
	
	
	@Override
	public void run() {
		try {
			this.authCheckService.check();
		} catch (Exception e) {
			if (logger.isWarnEnabled())
	        {
	        	logger.warn(StrUtils.showError(e));
	        }
		}
	}
}
