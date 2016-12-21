package com.wangjubao.app.others.job;

import java.util.Map;

import org.apache.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.wangjubao.app.others.service.IHisTradeCsvImportService;
import com.wangjubao.core.job.WangjubaoJob;
import com.wangjubao.core.util.SynContext;

/**
 * 历史交易导入
 * @author john_huang
 *
 */
@Deprecated
public class HisImpTradeDataJob extends WangjubaoJob {
	private final static Logger logger = Logger.getLogger(HisImpTradeDataJob.class);
	public static String SELLERID="SELLERID";
	public static String BASEPATH="BASEPATH";
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		Map map = context.getJobDetail().getJobDataMap();
		Long sellerId = Long.parseLong(map.get(SELLERID).toString());
		String basePath = map.get(BASEPATH).toString();
		try{
			IHisTradeCsvImportService service = (IHisTradeCsvImportService)SynContext.getObject("hisTradeCsvImportService");
			service.jobHisImportTrade(sellerId, basePath);
		}catch(Exception e){
			logger.info(e.getMessage(),e);
			/*
			MonitorVO vo = new MonitorVO();
			vo.setBusinessType(JobGroupConstant.IMPORT_DATA_SELLER_TRADE);
			vo.setSellerId(sellerId);
			vo.setMonitorLevel(MonitorVO.monitorLevel_1);
			vo.setMonitorDesc("sellerId=" + sellerId + "basePath=" + basePath + "执行历史数据导入解析csv入库步骤异常");
			vo.setStackTrace(ex);
			MonitorUtil.insertMonitorMsgVO(vo);
			*/
		}
	}

}
