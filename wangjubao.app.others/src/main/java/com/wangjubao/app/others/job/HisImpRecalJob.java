package com.wangjubao.app.others.job;

import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.SchedulerException;

import com.wangjubao.core.domain.syn.HisImport;
import com.wangjubao.core.domain.syn.HisImportProcess;
import com.wangjubao.core.job.WangjubaoJob;
import com.wangjubao.core.service.basic.ICommonService;
import com.wangjubao.core.util.JobGroupConstant;
import com.wangjubao.core.util.SynContext;

/**
 * 历史数据中间表重算job
 * 
 * @author Administrator
 */
@Deprecated
public class HisImpRecalJob extends WangjubaoJob {
    private transient final static Logger logger   = Logger.getLogger("others");
    public static String                  SELLERID = "SELLERID";
    public static String                  pkId     = "pkId";

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        // 重算卖家所有数据
        Map map = context.getJobDetail().getJobDataMap();
        Long sellerId = Long.parseLong(map.get(SELLERID).toString());
        HisImportProcess process;
        Long pkId = Long.parseLong(map.get("pkId").toString());
        ICommonService commonService = (ICommonService) SynContext.getObject("commonService");
        try {
            context.getScheduler().pauseJob(
                    new JobKey(sellerId.toString(), JobGroupConstant.DAY_ADDUP_DATE_CAL));

            HisImport imp = commonService.queryHisImportById(pkId, sellerId);
            imp.setStatus(HisImport.STATUS_3);
            imp.setUpdateTime(new Date());
            commonService.updateHisImportById(imp);

            process = new HisImportProcess();
            process.setParentId(pkId);
            process.setProcessDesc("开始进行中间表计算");
            process.setCreated(new Date());
            process.setSellerId(imp.getSellerId());
            commonService.insertHisImportProcess(process);
            /*
             * IRebuildAllTableService rebuil = (IRebuildAllTableService)
             * SynContext.getObject("rebuildAllTableService");
             * rebuil.allBuild(sellerId.toString()); try { OtherJobServer
             * jobService
             * =(OtherJobServer)SynContext.getObject("otherJobServiceImpl");
             * jobService.saveSellerLuceneBuild(sellerId, pkId.toString()); }
             * catch (Exception ex) { process = new HisImportProcess();
             * process.setParentId(pkId);
             * process.setProcessDesc("创建同重建lucene任务失败"); process.setCreated(new
             * Date()); process.setSellerId(imp.getSellerId());
             * process.setStatus(1);
             * commonService.insertHisImportProcess(process); }
             */
            process = new HisImportProcess();
            process.setParentId(pkId);
            process.setProcessDesc("中间表计算完成");
            process.setCreated(new Date());
            process.setSellerId(imp.getSellerId());
            process.setStatus(0);
            commonService.insertHisImportProcess(process);

            imp.setStatus(HisImport.STATUS_6);
            imp.setUpdateTime(new Date());
            commonService.updateHisImportById(imp);

        } catch (Throwable e) {
            logger.info("", e);
            process = new HisImportProcess();
            process.setParentId(pkId);
            process.setProcessDesc("中间表计算失败" + e.toString());
            process.setCreated(new Date());
            process.setStatus(1);
            process.setSellerId(sellerId);
            commonService.insertHisImportProcess(process);
        } finally {
            try {
                context.getScheduler().resumeJob(
                        new JobKey(sellerId.toString(), JobGroupConstant.DAY_ADDUP_DATE_CAL));
            } catch (SchedulerException e) {
                logger.info("", e);
            }

        }
    }

}
