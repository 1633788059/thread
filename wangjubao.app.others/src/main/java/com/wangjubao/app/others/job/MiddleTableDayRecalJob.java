package com.wangjubao.app.others.job;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.wangjubao.core.domain.basic.MonitorVO;
import com.wangjubao.core.domain.seller.TaobaoUser;
import com.wangjubao.core.job.WangjubaoJob;
import com.wangjubao.core.service.basic.MonitorUtil;
import com.wangjubao.core.service.datacalculate.IPreCalculateEngineService;
import com.wangjubao.core.service.seller.ITaobaoService;
import com.wangjubao.core.util.JobGroupConstant;
import com.wangjubao.core.util.SynContext;
import com.wangjubao.framework.util.DateUtil;

/**
 * 重算近3天的买家和卖家中间表的数据
 * 
 * @author admin
 */
@Deprecated
@DisallowConcurrentExecution
public class MiddleTableDayRecalJob extends WangjubaoJob {
    private transient final static Logger logger   = Logger.getLogger("others");
    public static String                  SELLERID = "SELLERID";

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        Map map = context.getJobDetail().getJobDataMap();
        Long userId = Long.parseLong(map.get(SELLERID).toString());
        logger.info("卖家=" + userId + "quartz==开始执行,中间表计算" + System.currentTimeMillis());
        try {
            ITaobaoService taobaoService = (ITaobaoService) SynContext.getObject("taobaoService");
            TaobaoUser tu = taobaoService.queryTaobaoUserById(userId);
            if (tu == null)
                return;
            // 不需要同步
            //			if (StringUtils.isNotBlank(tu.getSyncStatus()) && tu.getSyncStatus().equals("1"))
            //				return;
            // addby huangxiaodong 20121031 增加check交易数据
            if (tu.getShopType().intValue() == TaobaoUser.SHOP_TYPE_TAOBAO) {
                logger.info("开始check 卖家=" + userId + " 本地和taobao之间的交易数据");

                //				ITradeCheckService checkService = (ITradeCheckService) SynContext.getObject("tbTradeCheckServiceImpl");
                //				String dateStr = DateUtil.format(DateUtil.add(new Date(), Calendar.DATE, -1), "yyyy-MM-dd");
                //				try {
                //					checkService.onCheckAndAdjustByTradeCreateTime(userId, DateUtil.parse(dateStr + " 00:00:00", "yyyy-MM-dd HH:mm:ss"), DateUtil
                //							.parse(dateStr + " 23:59:59", "yyyy-MM-dd HH:mm:ss"));
                //				} catch (Exception ex) {
                //					logger.info("卖家=" + userId + "交易数据check异常", ex);
                //				}
                logger.info("完成check 卖家=" + userId + " 本地和taobao之间的交易数据");
            }

            IPreCalculateEngineService pre = (IPreCalculateEngineService) SynContext
                    .getObject("preCalculateEngineService");
            // 重算近3天的买家和卖家中间表的数据
            pre.calculateHisdataBySellerIdDates(userId,
                    DateUtil.add(new Date(), Calendar.DATE, -3), new Date());
        } catch (Exception ex) {
            logger.info("中间表计算错误", ex);
            MonitorVO vo = new MonitorVO();
            vo.setBusinessType(JobGroupConstant.DAY_ADDUP_DATE_CAL);
            vo.setSellerId(userId);
            vo.setMonitorLevel(MonitorVO.monitorLevel_1);
            vo.setMonitorDesc("sellerId=" + userId + "执行中间表数据重算异常");
            vo.setStackTrace(ex);
            MonitorUtil.insertMonitorMsgVO(vo);
        }
        logger.info("卖家=" + userId + " quartz==完成执行,中间表计算" + System.currentTimeMillis());
    }

    public static void main(String args[]) {
        System.out.println(DateUtil.add(new Date(), Calendar.DATE, -3));
    }

}
