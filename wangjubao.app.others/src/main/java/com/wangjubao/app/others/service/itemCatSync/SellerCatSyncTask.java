package com.wangjubao.app.others.service.itemCatSync;

import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.common.util.StrUtils;

@Service
public class SellerCatSyncTask implements Runnable{

	protected transient final static Logger logger =  Logger.getLogger("sellercats");
	
	@Autowired
	private SellerCatSyncService sellerCatSyncService;

	private String sellerNick;

	private Map<String, Boolean> runnableSellers;
	
	public SellerCatSyncTask() {
		super();
	}
	
	public SellerCatSyncTask(String sellerNick,
			Map<String, Boolean> runnableSellers) {
		this.sellerNick = sellerNick;
        this.runnableSellers = runnableSellers;
	}
	@Override
	public void run() {
		sellerCatSyncService = (SellerCatSyncService)SynContext.getObject("sellerCatSyncService");
		try {
			this.sellerCatSyncService.sync(sellerNick);
		} catch (Exception e) {
			if (logger.isInfoEnabled())
	        {
	        	logger.warn(StrUtils.showError(e));
	        }
		}finally{
			synchronized (runnableSellers)
            {
//    	        if (logger.isInfoEnabled())
//    	        {
//    	        	logger.info(" ----------------runnableSellers.add(sellerNick) start: " + runnableSellers);
//    	        }
    	        if(runnableSellers.containsKey(sellerNick))
    	        {
    	        	runnableSellers.put(sellerNick, false);
    	        }
//    	        if (logger.isInfoEnabled())
//    	        {
//    	        	logger.info(" ----------------runnableSellers.add(sellerNick) end: " + runnableSellers);
//    	        }
            }
		}
	}

}
