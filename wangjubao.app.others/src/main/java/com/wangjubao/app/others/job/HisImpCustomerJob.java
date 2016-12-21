package com.wangjubao.app.others.job;

import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.wangjubao.core.domain.basic.MonitorVO;
import com.wangjubao.core.domain.syn.HisImport;
import com.wangjubao.core.domain.syn.HisImportProcess;
import com.wangjubao.core.job.WangjubaoJob;
import com.wangjubao.core.service.basic.ICommonService;
import com.wangjubao.core.service.basic.MonitorUtil;
import com.wangjubao.core.service.syn.IFacadeService;
import com.wangjubao.core.util.JobGroupConstant;
import com.wangjubao.core.util.SynContext;

/**
 * 历史客户信息处理
 * 
 * @author john_huang
 */
@Deprecated
public class HisImpCustomerJob extends WangjubaoJob {
    private transient final static Logger logger = Logger.getLogger("others");

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        Map map = context.getJobDetail().getJobDataMap();
        Long sellerId = Long.parseLong(map.get("sellerId").toString());
        Long pkId = Long.parseLong(map.get("pkId").toString());
        try {
            IFacadeService facadeService = (IFacadeService) SynContext.getObject("facadeService");
            ICommonService commonService = (ICommonService) SynContext.getObject("commonService");

            HisImport imp = commonService.queryHisImportById(pkId, sellerId);
            imp.setStatus(HisImport.STATUS_5);
            imp.setUpdateTime(new Date());
            commonService.updateHisImportById(imp);

            HisImportProcess process = new HisImportProcess();
            process.setSellerId(imp.getSellerId());
            process.setParentId(pkId);
            process.setProcessDesc("开始进行买家信息全部同步");
            process.setCreated(new Date());
            commonService.insertHisImportProcess(process);

            facadeService.doSynSellerUserInfo(sellerId);

            imp = commonService.queryHisImportById(pkId, sellerId);
            imp.setStatus(HisImport.STATUS_6);
            imp.setUpdateTime(new Date());
            commonService.updateHisImportById(imp);

            process = new HisImportProcess();
            process.setParentId(pkId);
            process.setProcessDesc("买家信息全部同步发送完成");
            process.setCreated(new Date());
            process.setStatus(0);
            process.setSellerId(imp.getSellerId());
            commonService.insertHisImportProcess(process);
        } catch (Exception ex) {
            MonitorVO vo = new MonitorVO();
            vo.setBusinessType(JobGroupConstant.DAY_ADDUP_DATE_CAL);
            vo.setSellerId(sellerId);
            vo.setMonitorLevel(MonitorVO.monitorLevel_1);
            vo.setMonitorDesc("sellerId=" + sellerId + "pkId=" + pkId + "执行历史数据导入买家信息全部同步异常");
            vo.setStackTrace(ex);
            MonitorUtil.insertMonitorMsgVO(vo);
        }
    }

}
