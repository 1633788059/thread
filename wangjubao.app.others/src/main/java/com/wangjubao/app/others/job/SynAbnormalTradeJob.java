package com.wangjubao.app.others.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.wangjubao.core.domain.basic.MonitorVO;
import com.wangjubao.core.domain.job.JobException;
import com.wangjubao.core.service.basic.MonitorUtil;
import com.wangjubao.core.service.job.IAbnormalTradeJobService;
import com.wangjubao.core.util.JobGroupConstant;
import com.wangjubao.core.util.SynContext;

@Deprecated
public class SynAbnormalTradeJob implements Job {

    /**
     * 同步异常物流订单
     */
    @Override
    public void execute(JobExecutionContext arg0) throws JobExecutionException {
        try {
            IAbnormalTradeJobService abnormalTradeJobService = (IAbnormalTradeJobService) SynContext
                    .getObject("abnormalTradeJobService");
            abnormalTradeJobService.synAbnormalTradeJob();
        } catch (Exception ex) {
            MonitorVO vo = new MonitorVO();
            vo.setBusinessType(JobGroupConstant.SYN_ABNORMAL_TRADE);
            vo.setSellerId(-1L);
            vo.setMonitorLevel(MonitorVO.monitorLevel_1);
            vo.setMonitorDesc("执行同步异常物流订单异常" + ex.getMessage());
            vo.setStackTrace(ex);
            MonitorUtil.insertMonitorMsgVO(vo);
            throw new JobException(vo, ex);
        }
    }
}
