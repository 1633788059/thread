package com.wangjubao.app.others.service.promotionsync;

import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.app.others.service.itemCatSync.SellerCatSyncService;
import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.common.util.StrUtils;

@Service
public class PromotionSyncTask implements Runnable{

	protected transient final static Logger logger =  Logger.getLogger("promotionsync");
	
	@Autowired
	private PromotionSyncService promotionSyncService;

	private String sellerNick;

	private Map<String, Boolean> runnableSellers;
	
	public PromotionSyncTask() {
		super();
	}
	
	public PromotionSyncTask(String sellerNick,
			Map<String, Boolean> runnableSellers) {
		this.sellerNick = sellerNick;
        this.runnableSellers = runnableSellers;
	}
	@Override
	public void run() {
		promotionSyncService = (PromotionSyncService)SynContext.getObject("promotionSyncService");
		try {
			this.promotionSyncService.sync(sellerNick);
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
