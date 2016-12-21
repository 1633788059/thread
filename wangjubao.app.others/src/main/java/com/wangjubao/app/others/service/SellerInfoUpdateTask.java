package com.wangjubao.app.others.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.dolphin.common.util.StrUtils;


@Service
public class SellerInfoUpdateTask implements Runnable {

	protected transient final static Logger logger = LoggerFactory
			.getLogger(SellerInfoUpdateTask.class);
	@Autowired
	private SellerInfoMap sellerInfoMap;
	
	@Override
	public void run() {
		try {
			this.sellerInfoMap.loadAllSellerDataAndSave();
		} catch (Exception e) {
			if (logger.isWarnEnabled())
	        {
	        	logger.warn(StrUtils.showError(e));
	        }
		}
	}

}
