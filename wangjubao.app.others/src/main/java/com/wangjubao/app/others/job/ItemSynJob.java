package com.wangjubao.app.others.job;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.wangjubao.core.domain.basic.MonitorVO;
import com.wangjubao.core.domain.job.JobException;
import com.wangjubao.core.job.WangjubaoJob;
import com.wangjubao.core.service.basic.MonitorUtil;
import com.wangjubao.core.service.system.ISyncItemService;
import com.wangjubao.core.util.JobGroupConstant;
import com.wangjubao.core.util.SynContext;

/**
 * 商品目录同步
 * 
 * @author john_huang
 */
@Deprecated
@DisallowConcurrentExecution
public class ItemSynJob extends WangjubaoJob {
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            ISyncItemService itemService = (ISyncItemService) SynContext
                    .getObject("syncItemService");
            itemService.onSyncAllSellerItem();
        } catch (Exception ex) {
            MonitorVO vo = new MonitorVO();
            vo.setBusinessType(JobGroupConstant.SELLER_ITEM_SYN);
            vo.setSellerId(-1L);
            vo.setMonitorLevel(MonitorVO.monitorLevel_1);
            vo.setMonitorDesc("执行商品目录的数据同步异常" + ex.getMessage());
            vo.setStackTrace(ex);
            MonitorUtil.insertMonitorMsgVO(vo);
            throw new JobException(vo, ex);
        }
    }

}
