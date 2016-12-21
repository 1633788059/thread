package com.wangjubao.app.others.service.umptag;

import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.common.util.StrUtils;

@Service
public class UmpTagTask implements Runnable{

	protected transient final static Logger logger =  Logger.getLogger("umptag");
	
	@Autowired
	private UmpTagService umpTagService;

	private SellerDo sellerDo;

	private Map<Long, Boolean> runnableSellers;
	
	public UmpTagTask() {
		super();
	}
	
	public UmpTagTask(SellerDo sellerDo,
			Map<Long, Boolean> runnableSellers) {
		this.sellerDo = sellerDo;
        this.runnableSellers = runnableSellers;
	}
	@Override
	public void run() {
		umpTagService = (UmpTagService)SynContext.getObject("umpTagService");
		try {
			this.umpTagService.sync(sellerDo);
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
    	        if(runnableSellers.containsKey(sellerDo.getId()))
    	        {
    	        	runnableSellers.put(sellerDo.getId(), false);
    	        }
//    	        if (logger.isInfoEnabled())
//    	        {
//    	        	logger.info(" ----------------runnableSellers.add(sellerNick) end: " + runnableSellers);
//    	        }
            }
		}
	}

}
