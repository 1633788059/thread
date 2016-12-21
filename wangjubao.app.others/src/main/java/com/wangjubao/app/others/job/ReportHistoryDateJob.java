package com.wangjubao.app.others.job;

import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * @author ckex 说明：sms_log和email_log计算过效果报千的数据进行迁移
 */
@Deprecated
public class ReportHistoryDateJob implements Job {

    public transient final static Logger logger = Logger.getLogger(ReportHistoryDateJob.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        /*
         * Long time=System.currentTimeMillis();
         * logger.info("a job have started......."+time); ReportFlowNew
         * reportService = new ReportFlowNew(); reportService.splitTable(); Long
         * end=System.currentTimeMillis();
         * logger.info("a job have finished......."
         * +time+"==total time"+(end-time)/1000+" seconds");
         */
    }

}
