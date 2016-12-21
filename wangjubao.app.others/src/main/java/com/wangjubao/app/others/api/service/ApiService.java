/**
 * 
 */
package com.wangjubao.app.others.api.service;

import java.util.Date;

import com.taobao.api.domain.Task;
import com.taobao.api.domain.Trade;

/**
 * @author ckex created 2014-1-15 下午3:50:48
 * @explain -
 */
public interface ApiService {

    /**
     * @param sellerNick 店铺名
     * @param tid 淘宝订单号
     * @return com.taobao.api.domain.Trade
     */
    Trade loadTarde(String sellerNick, Long tid);
    
    /**
     * taobao.topats.promotion.coupondetail.get 异步查询店铺优惠券发放信息
     */
    Task getCoupondetail(Long sellerId,Long couponId,String state,Date startTime,Date endTime);
    
    /**
     * taobao.topats.result.get 获取异步任务结果 
     */
    Task getCouponreport(Long sellerId,Long taskId);
}
