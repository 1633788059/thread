package com.wangjubao.app.others.service.downcenter;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.model.SysSyntaskDo;
import com.wangjubao.dolphin.biz.service.SellerService;
import com.wangjubao.dolphin.common.util.StrUtils;

@Service
public class DownCenterCleanTask implements Runnable{

	static Logger logger = Logger.getLogger("downcenter");
	
	@Autowired
	private TaskCodeTypeService taskCodeTypeService;
	@Autowired
	private SellerService sellerService;
	@Override
	public void run() {
		/*
		try {
			PageQuery pageQuery = new PageQuery(0, 50);
			Integer count = 1;

			SysSyntaskDo record = new SysSyntaskDo();
			record.setStatus("done");
			record.setGmtCreate(new Date());
//			record.setCode(TradeTaskServiceImpl.INIT_TASK_CODE);  //remove all task files
			List<SysSyntaskDo> list = sellerService.listSysSyntaskList(record,
					pageQuery, count);
			for (SysSyntaskDo task : list) {		
				DownCenterService centerService = this.taskCodeTypeService.getServiceByCodeType(task.getCode());
				if (centerService!=null) {
					centerService.clean(task);
				}
			}
		} catch (Exception e) {
			if (logger.isInfoEnabled())
	        {
	        	logger.warn(StrUtils.showError(e));
	        }
		}
		*/
	}

}
