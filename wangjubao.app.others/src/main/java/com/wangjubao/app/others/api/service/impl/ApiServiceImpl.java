/**
 * 
 */
package com.wangjubao.app.others.api.service.impl;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.taobao.api.ApiException;
import com.taobao.api.DefaultTaobaoClient;
import com.taobao.api.TaobaoClient;
import com.taobao.api.domain.Task;
import com.taobao.api.domain.Trade;
import com.taobao.api.internal.util.TaobaoUtils;
import com.taobao.api.request.TopatsPromotionCoupondetailGetRequest;
import com.taobao.api.request.TopatsResultGetRequest;
import com.taobao.api.response.TopatsPromotionCoupondetailGetResponse;
import com.taobao.api.response.TopatsResultGetResponse;
import com.taobao.api.response.TradeGetResponse;
import com.wangjubao.app.others.api.service.ApiService;
import com.wangjubao.app.others.service.couponReport.impl.CouponSellerMap;
import com.wangjubao.dolphin.biz.dao.CouponTaskDao;
import com.wangjubao.dolphin.biz.dao.JdpTbTradeDao;
import com.wangjubao.dolphin.biz.model.CouponTaskDo;
import com.wangjubao.dolphin.biz.model.JdpTbTradeDo;
import com.wangjubao.dolphin.biz.model.extend.AppParameterTaobao;
import com.wangjubao.dolphin.common.util.StrUtils;
import com.wangjubao.framework.util.GlobalUtil;

/**
 * @author ckex created 2014-1-15 下午3:51:19
 * @explain -
 */
@Service("apiService")
public class ApiServiceImpl implements ApiService {
    
    static final Logger                               logger       = Logger.getLogger("couponcreatetask");

    private static SimpleDateFormat sdf    = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    @Autowired
    @Qualifier("pushJdpTbTradeDao")
    private JdpTbTradeDao jdpTbTradeDao;
    @Autowired
    private CouponTaskDao couponTaskDao;

    @Override
    public Trade loadTarde(String sellerNick, Long tid) {
        JdpTbTradeDo jdpTbTradeDo = jdpTbTradeDao.loadTradeByTid(tid, sellerNick);
        if (jdpTbTradeDo == null) {
            return null;
        }
        String json = jdpTbTradeDo.getJdpResponse();
        if (StringUtils.isBlank(json)) {
            return null;
        }
        TradeGetResponse response = null;
        try {
            response = TaobaoUtils.parseResponse(json, TradeGetResponse.class);
        } catch (ApiException e) {
            e.printStackTrace();
        }
        if (response == null) {
            return null;
        }
        return response.getTrade();
    }

    @Override
    public Task getCoupondetail(Long sellerId, Long couponId, String state, Date startTime, Date endTime)
    {
        AppParameterTaobao appParameterTaobao = CouponSellerMap.appParameterTaobaoMap.get(sellerId);
        if(appParameterTaobao==null){
            return null;
        }
        TopatsPromotionCoupondetailGetRequest req=new TopatsPromotionCoupondetailGetRequest();
        req.setCouponId(couponId);
        if(state!=null){
            req.setState(state);
        }
        req.setStartTime(startTime);
        req.setEndTime(endTime);
        TopatsPromotionCoupondetailGetResponse response;
        Task task = null;
        try
        {
            if(appParameterTaobao.getExpiresIn()!=""){
                
            
                TaobaoClient taobaoClient = new DefaultTaobaoClient(GlobalUtil.clientURL, appParameterTaobao.getAppkey(), appParameterTaobao.getSecret(), "json");
                response = taobaoClient.execute(req, appParameterTaobao.getAccessToken());
                if (response.isSuccess())
                {
                    task = response.getTask();
                } else
                {
                    if(response.getSubMsg()!=""&&response.getSubMsg().indexOf("相同的任务已经存在")>-1){
                    	List<CouponTaskDo> lisTaskDos = this.couponTaskDao.couponTasks(sellerId,Long.valueOf( response.getSubMsg().split("=")[1]));
                    	if(lisTaskDos!=null&&lisTaskDos.size()>0){
                    		
                    	}else{
	                        task = new Task();
	                        task.setTaskId(Long.valueOf( response.getSubMsg().split("=")[1]));
                    	}
                        logger.info(Long.valueOf( response.getSubMsg().split("=")[1])+"相同任务已经存在，获取对应的id");
                    }else if(response.getSubMsg()!=null&&response.getSubMsg()!=""&&response.getSubMsg().indexOf("异步任务结果为空")>-1){
                        task = new Task();
                        task.setStatus("empty");
                    }else if(response.getSubMsg()!=null&&response.getSubMsg()!=""&&response.getSubMsg().indexOf("该任务不存在")>-1){
                        task = new Task();
                        task.setStatus("notask");
                    }
                    logger.error(sellerId+"???????"+startTime+">>>>end>>>"+endTime+">>>>调用生成优惠券任务接口发生错误，错误信息》》》》》》"+response.getMsg()+response.getSubMsg()+response.getBody());
                }

            }
        } catch (Exception ex)
        {
            String error = StrUtils.showError(ex);
            logger.error(startTime+">>>>end>>>"+endTime+">>>>调用生成优惠券任务接口发生异常，异常信息》》》》》》"+error);
        }
        return task;
    }

    @Override
    public Task getCouponreport(Long sellerId, Long taskId)
    {
        AppParameterTaobao appParameterTaobao = CouponSellerMap.appParameterTaobaoMap.get(sellerId);
        if(appParameterTaobao==null){
        	logger.info("###### appParameterTaobao  is null sellerId:"+sellerId);
            return null;
        }
        TopatsResultGetRequest req = new TopatsResultGetRequest();
        req.setTaskId(taskId);
        TopatsResultGetResponse response ;
        Task task = null;
        try
        {
            if(appParameterTaobao.getExpiresIn()!=""){
                
            
                TaobaoClient taobaoClient = new DefaultTaobaoClient(GlobalUtil.clientURL, appParameterTaobao.getAppkey(), appParameterTaobao.getSecret(), "json");
                response = taobaoClient.execute(req, appParameterTaobao.getAccessToken());
                if (response.isSuccess() &&response.getTask()!=null&& response.getTask().getStatus()!=null && response.getTask().getStatus().equals("done")) 
                {
                    task = response.getTask();
                } else
                {
                	if (response.isSuccess() &&response.getTask()!=null&& response.getTask().getStatus()!=null && response.getTask().getStatus().equals("doing") ) {
						task=response.getTask();
					}
                    if(response.getSubMsg()!=null&&response.getSubMsg()!=""&&response.getSubMsg().indexOf("异步任务结果为空")>-1){
                        task = new Task();
                        task.setStatus("empty");
                    }
                    if(response.getSubMsg()!=null&&response.getSubMsg()!=""&&response.getSubMsg().indexOf("该任务不存在")>-1){
                        task = new Task();
                        task.setStatus("notask");
                    }
                    logger.error(sellerId+"???????"+"调用消费优惠券任务接口发生错误，错误信息》》》》》》"+response.getBody()+response.getMsg()+response.getSubMsg()+response.getBody()+"  ## TASK:"+response.getTask());
                }

            }
        } catch (Exception ex)
        {
        	ex.printStackTrace();
            String error = StrUtils.showError(ex);
            logger.error("调用消费优惠券任务接口发生异常，异常信息》》》》》》"+error);
        }
        return task;
    }

}
