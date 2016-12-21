package com.wangjubao.app.others.job;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.wangjubao.core.domain.basic.MonitorVO;
import com.wangjubao.core.job.WangjubaoJob;
import com.wangjubao.core.service.basic.MonitorUtil;
import com.wangjubao.core.service.seller.ITaobaoService;
import com.wangjubao.core.util.JobGroupConstant;
import com.wangjubao.core.util.SynContext;

/**
 * 每日定时发送邮件日报
 * 
 * @author admin
 */
@Deprecated
@DisallowConcurrentExecution
public class SystemDayReportJob extends WangjubaoJob {
    private transient final static Logger logger = Logger.getLogger("others");

    @Override
    public void execute(JobExecutionContext arg0) throws JobExecutionException {
        logger.info("quartz==开始执行,每日日报==");
        try {
            ITaobaoService taobaoService = (ITaobaoService) SynContext.getObject("taobaoService");
            taobaoService.queryEveryDayReportToUser(null);
        } catch (Exception ex) {
            logger.info("", ex);
            MonitorVO vo = new MonitorVO();
            vo.setBusinessType(JobGroupConstant.SYSTEM_DAY_REPORT);
            vo.setSellerId(-1L);
            vo.setMonitorLevel(MonitorVO.monitorLevel_1);
            vo.setMonitorDesc("执行日报发送异常" + ex.getMessage());
            vo.setStackTrace(ex);
            MonitorUtil.insertMonitorMsgVO(vo);
        }
        logger.info("current jinchengid=" + getPid());
        logger.info("quartz==完成执行,每日日报=");
    }

    private static int getPid() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        String name = runtime.getName(); // format: "pid@hostname"  
        try {
            return Integer.parseInt(name.substring(0, name.indexOf('@')));
        } catch (Exception e) {
            return -1;
        }
    }

}
