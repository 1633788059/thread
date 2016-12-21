package com.wangjubao.app.others.job;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.wangjubao.core.domain.basic.MonitorVO;
import com.wangjubao.core.job.WangjubaoJob;
import com.wangjubao.core.service.basic.MonitorUtil;
import com.wangjubao.core.service.syn.IHandRateSynService;
import com.wangjubao.core.util.JobGroupConstant;
import com.wangjubao.core.util.SynContext;
import com.wangjubao.framework.util.DateUtil;

/**
 * 评价同步 同步每个卖家的评价信息
 * 
 * @author john_huang
 */
@Deprecated
public class TradeRateSynJob extends WangjubaoJob {
    private transient final static Logger logger   = Logger.getLogger("others");
    public static String                  SELLERID = "SELLERID";

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        Map map = context.getJobDetail().getJobDataMap();
        Long userId = Long.parseLong(map.get(SELLERID).toString());
        //	System.out.println("TradeRateSynJob>>>>map.get(SELLERID) job>>>>>>"+userId);
        IHandRateSynService rateSyn = (IHandRateSynService) SynContext
                .getObject("handRateSynServiceImpl");
        try {
            Date begin = DateUtil.DATE_SDF.parse(DateUtil.DATE_SDF.format(DateUtil.add(new Date(),
                    Calendar.DATE, -1)));
            rateSyn.onDataSyn("", userId, begin, new Date());
        } catch (Exception ex) {
            logger.info("sellerId=" + userId + " 交易评价同步失败");
            MonitorVO vo = new MonitorVO();
            vo.setBusinessType(JobGroupConstant.TAOBAO_TRADE_RATE_SYN);
            vo.setSellerId(userId);
            vo.setMonitorLevel(MonitorVO.monitorLevel_1);
            vo.setMonitorDesc("执行评价数据同步异常" + ex.getMessage());
            vo.setStackTrace(ex);
            MonitorUtil.insertMonitorMsgVO(vo);
            throw new JobExecutionException("", ex);
        }
    }

}
