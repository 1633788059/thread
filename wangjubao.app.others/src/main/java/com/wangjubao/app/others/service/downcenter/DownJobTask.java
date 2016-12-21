package com.wangjubao.app.others.service.downcenter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.wangjubao.dolphin.biz.model.SysSyntaskDo;


public class DownJobTask implements Runnable{

	private final Logger logger = LoggerFactory.getLogger("downcenter");
	
	private DownCenterTask mainTask;
	
	private SysSyntaskDo sysSyntaskDo;
	
	private DownCenterService centerService;
	
	public  DownJobTask(SysSyntaskDo sysSyntaskDo,DownCenterService centerService, DownCenterTask mainTask) {
		this.sysSyntaskDo = sysSyntaskDo;
		this.centerService = centerService;
		this.mainTask = mainTask;
	}
	
	@Override
	public void run() {
//		taskCodeTypeService = (TaskCodeTypeService)SynContext.getObject("taskCodeTypeService");
		try {
//			DownCenterService centerService = this.taskCodeTypeService.getServiceByCodeType(sysSyntaskDo.getCode());
//			if (centerService!=null) {
				centerService.job(sysSyntaskDo);
//			}
		} catch (Throwable e) {
	        logger.error("Error occurs when execute task"+sysSyntaskDo.getTitle(), e);
		}finally{
			mainTask.releaseToken(sysSyntaskDo.getId());
		}
		
	}

}
