package com.wangjubao.app.others.job;

import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.wangjubao.core.job.WangjubaoJob;
import com.wangjubao.core.service.marketing.IBusinessTempDataClearService;
import com.wangjubao.core.util.SynContext;

/**
 * 清除一些业务中间表的数据 催付，发货通知，mq交易计算中间表
 * 
 * @author john_huang
 * 
 */
@Deprecated
@DisallowConcurrentExecution
public class BusinessTempTableClearJob extends WangjubaoJob  {
	private transient final static Logger logger = Logger.getLogger("others");

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {

		IBusinessTempDataClearService service =null;

		logger.info("clear outnotice data....");
		try {
			service= (IBusinessTempDataClearService) SynContext.getObject("businessTempDataClearService");
			service.clearOutNoticeData();
		} catch (Throwable ex) {
			logger.info("", ex);
		}
		logger.info("clear dunning data....");
		try {
			service.clearDunnintData();
		} catch (Exception ex) {
			logger.info("", ex);
		}
		logger.info("clear trade calculate data");
//		try {
//			service.clearTradeCalData();
//		} catch (Exception ex) {
//			logger.info("", ex);
//		}

	}

}
