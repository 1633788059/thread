package com.wangjubao.app.others.job;

import org.apache.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.wangjubao.core.domain.basic.MonitorVO;
import com.wangjubao.core.domain.job.JobException;
import com.wangjubao.core.job.WangjubaoJob;
import com.wangjubao.core.service.basic.MonitorUtil;
import com.wangjubao.core.service.system.INoticeService;
import com.wangjubao.core.util.JobGroupConstant;
import com.wangjubao.core.util.SynContext;

@Deprecated
public class NotifyBySmsJob extends WangjubaoJob {
    private transient final static Logger logger = Logger.getLogger("others");

    private transient final static Logger log    = Logger.getLogger(WangjubaoJob.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        logger.info("quartz==开始执行,每日提醒==");
        log.info("quartz==开始执行,每日提醒==");
        try {
            INoticeService noticeService = (INoticeService) SynContext.getObject("noticeService");
            noticeService.notifyRemind();
        } catch (Exception ex) {
            MonitorVO vo = new MonitorVO();
            vo.setBusinessType(JobGroupConstant.NOTIFY_BY_SMS);
            vo.setSellerId(-1L);
            vo.setMonitorLevel(MonitorVO.monitorLevel_1);
            vo.setMonitorDesc("执行短信邮件余额扫描失败" + ex.getMessage());
            vo.setStackTrace(ex);
            MonitorUtil.insertMonitorMsgVO(vo);
            throw new JobException(vo, ex);
        }
        logger.info("quartz==完成执行,每日提醒=");
    }

}
