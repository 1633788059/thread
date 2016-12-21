package com.wangjubao.app.others.job;

import java.util.Map;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.wangjubao.app.others.service.ReadHistoryDataService;
import com.wangjubao.dolphin.biz.service.job.AppContext;
import com.wangjubao.dolphin.biz.service.job.JobConstant;
import com.wangjubao.dolphin.biz.service.job.WangjubaoJob;

/**
 * @author ckex created 2013-6-15 下午4:59:45
 * @explain -
 */
public class HistoryDataImportJob extends WangjubaoJob {

    // TODO change
    @SuppressWarnings("rawtypes")
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        logger.info(" 开始执行,卖家历史数据导入. ");
        Map map = context.getJobDetail().getJobDataMap();
        Long sellerId = Long.parseLong(map.get(JobConstant.KEY_SELLER_ID).toString());
        long t = System.currentTimeMillis();
        try {
            ReadHistoryDataService readHistoryDataService = (ReadHistoryDataService) AppContext
                    .getBean("readHistoryDataService");
            readHistoryDataService.executeHistoryDataImport(sellerId);
            logger.info("完成执行,历史数据导入:  in " + (System.currentTimeMillis() - t) / 1000.0
                    + " seconds .");
        } catch (Exception ex) {
            logger.info(" 卖家历史数据导入失败 . ");
            logger.info(ex.getMessage(), ex);
        }

    }

}
