package com.wangjubao.app.others.job;

import org.apache.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.wangjubao.core.domain.basic.MonitorVO;
import com.wangjubao.core.domain.job.JobException;
import com.wangjubao.core.job.WangjubaoJob;
import com.wangjubao.core.service.analyse.IReportService;
import com.wangjubao.core.util.SynContext;

/**
 * Group : wangjubao.com. 说明：报告模块后台接口
 * 
 * @author haibao
 * @create 11 3, 2012 6:59:57 PM
 */
@Deprecated
public class ReportJobForWeb extends WangjubaoJob {
    public transient final static Logger logger   = Logger.getLogger("others");
    public static String                 SELLERID = "SELLERID";
    public static String                 EVENTID  = "EVENTID";
    public static String                 TYPE     = "TYPE";

    /**
     * 执行报告轮询任务
     */
    public void execute(JobExecutionContext context) throws JobExecutionException {
        Long time = System.currentTimeMillis();
        logger.info("a report job have started......." + time);
        JobDataMap jobDataMap = context.getMergedJobDataMap();
        IReportService reportService = (IReportService) SynContext.getObject("reportService");
        String sellerid = (String) jobDataMap.get(SELLERID);
        String eventid = (String) jobDataMap.get(EVENTID);
        String type = (String) jobDataMap.get(TYPE);
        try {
            reportService.insertEventSumInSellerId(sellerid, eventid, type);
        } catch (Exception ex) {
            MonitorVO vo = new MonitorVO();
            //				vo.setBusinessType(JobGroupConstant.REPORT_AN_ACTIVITY);
            //				vo.setSellerId(Long.valueOf(sellerid));
            //				vo.setMonitorLevel(MonitorVO.monitorLevel_1);
            //				vo.setMonitorDesc("执行报表统计的异常"+ex.getMessage());
            //				vo.setStackTrace(ex);
            //				MonitorUtil.insertMonitorMsgVO(vo);
            throw new JobException(vo, ex);
        }
        Long end = System.currentTimeMillis();
        logger.info("a report job have finished......." + time + "==total time" + (end - time)
                / 1000 + " seconds");
    }

}
