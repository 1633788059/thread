package com.wangjubao.app.others.service.impl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.drools.lang.DRLParser.result_key_return;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.taobao.api.ApiException;
import com.taobao.api.DefaultTaobaoClient;
import com.taobao.api.TaobaoClient;
import com.taobao.api.domain.Trade;
import com.taobao.api.domain.TransitStepInfo;
import com.taobao.api.request.LogisticsTraceSearchRequest;
import com.taobao.api.request.TradeFullinfoGetRequest;
import com.taobao.api.request.TradeGetRequest;
import com.taobao.api.request.TradesSoldGetRequest;
import com.taobao.api.request.UserGetRequest;
import com.taobao.api.response.LogisticsTraceSearchResponse;
import com.taobao.api.response.TradeFullinfoGetResponse;
import com.taobao.api.response.TradeGetResponse;
import com.taobao.api.response.TradesSoldGetResponse;
import com.taobao.api.response.UserGetResponse;
import com.wangjubao.app.others.api.service.ApiService;
import com.wangjubao.app.others.service.AnomalyLogisticsService;
import com.wangjubao.core.dao.wangwang.IAbnormalTradeDao;
import com.wangjubao.core.domain.authorization.TaobaoSubUser;
import com.wangjubao.core.domain.seller.AbnormalTrade;
import com.wangjubao.core.service.authorization.AuthoriztionService;
import com.wangjubao.core.service.basic.IUserService;
import com.wangjubao.core.service.wangwang.IAbnormalTradeService;
import com.wangjubao.dolphin.biz.common.constant.BaseType;
import com.wangjubao.dolphin.biz.common.constant.ProductType;
import com.wangjubao.dolphin.biz.common.constant.TradeStatus;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dal.extend.bo.SellerBo;
import com.wangjubao.dolphin.biz.common.constant.SellerType;
import com.wangjubao.dolphin.biz.dao.AbnormalTradeDao;
import com.wangjubao.dolphin.biz.dao.BuyerDao;
import com.wangjubao.dolphin.biz.dao.LogisticsDetailDao;
import com.wangjubao.dolphin.biz.dao.SellerAbnormalTradeLogDao;
import com.wangjubao.dolphin.biz.dao.SellerAbnormalTradeRuleDao;
import com.wangjubao.dolphin.biz.dao.SellerDao;
import com.wangjubao.dolphin.biz.dao.SellerProductSubscribeDao;
import com.wangjubao.dolphin.biz.dao.TradeDao;
import com.wangjubao.dolphin.biz.model.AbnormalTradeDo;
import com.wangjubao.dolphin.biz.model.JsonAbnormalSourceRule;
import com.wangjubao.dolphin.biz.model.LogisticsDetailDo;
import com.wangjubao.dolphin.biz.model.SellerAbnormalTradeLogDo;
import com.wangjubao.dolphin.biz.model.SellerAbnormalTradeRuleDo;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.biz.model.SellerProductSubscribeDo;
import com.wangjubao.dolphin.biz.model.TradeDo;
import com.wangjubao.dolphin.biz.model.extend.AppParameterTaobao;
import com.wangjubao.dolphin.biz.ons.model.LogisticsDetailTrace;
import com.wangjubao.dolphin.biz.service.CompanyService;
import com.wangjubao.dolphin.biz.service.SellerSessionKeyService;
import com.wangjubao.dolphin.biz.top.TOPClientFactory;
import com.wangjubao.dolphin.common.lang.Paginator;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.StrUtils;
import com.wangjubao.dolphin.common.util.date.DateUtils;
import com.wangjubao.framework.entity.BaseEntity;
import com.wangjubao.framework.util.DateUtil;

/**
 * @author ckex created 2013-6-22 - 上午10:45:23 AnomalyLogisticsServiceImpl.java
 * @explain -
 */
@Service("anomalyLogisticsService")
public class AnomalyLogisticsServiceImpl implements AnomalyLogisticsService {

    private transient final static Logger             logger         = LoggerFactory.getLogger("AnomalyLogistics");

    @Autowired
    private SellerBo                                  sellerBo;

    @Autowired
    private SellerDao                                 sellerDao;

    @Autowired
    private SellerProductSubscribeDao                 sellerProductSubscribeDao;

    @Autowired
    private SellerSessionKeyService                   sellerSessionKeyService;

    @Autowired
    @Qualifier("dolphinBuyerDao")
    private BuyerDao                                  buyerDao;

    @Autowired
    private SellerAbnormalTradeRuleDao                sellerAbnormalTradeRuleDao;

    @Autowired
    @Qualifier("abnormalTradeService")
    private IAbnormalTradeService                     abnormalTradeService;

    @Autowired
    @Qualifier("abnormalTradeDao")
    private IAbnormalTradeDao                         abnormalTradeDao;

    @Autowired
    @Qualifier("userService")
    private IUserService                              userService;

    @Autowired
    private CompanyService                            companyService;

    @Autowired
    private ApiService                                apiService;

    @Autowired
    private TradeDao                                  tradeDao;

    @Autowired
    @Qualifier("authorizationService")
    private AuthoriztionService                       authoriztionService;

    @Autowired
    private SellerAbnormalTradeLogDao                 sellerAbnormalTradeLogDao;
    
    @Autowired
    @Qualifier("dolphinAbnormalTradeDao")
    private AbnormalTradeDao                          dolphinAbnormalTradeDao;
    @Autowired
    private LogisticsDetailDao                        logisticsDetailDao;
    
    private ExecutorService  logisticsExecutor = Executors.newFixedThreadPool(32);
    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
    private static ConcurrentHashMap<Long, Boolean>  runningSellers = new ConcurrentHashMap<Long, Boolean>(); 

//    private static final Map<Long, AtomicBoolean>     CURRENT_STATUS = new ConcurrentHashMap<Long, AtomicBoolean>();

    //    private static final int                MAX_SIZE       = 5;
    //
    //    private static ThreadPoolExecutor       executor       = new ThreadPoolExecutor(
    //                                                                   MAX_SIZE,
    //                                                                   MAX_SIZE,
    //                                                                   100,
    //                                                                   TimeUnit.MILLISECONDS,
    //                                                                   new LinkedBlockingQueue<Runnable>(),
    //                                                                   new ThreadPoolExecutor.AbortPolicy()); // 表示拒绝任务并抛出异常

//    private static final Map<String, ExecutorService> EXECUTOR_MAP   = new ConcurrentHashMap<String, ExecutorService>();

//    private String                                    clientURL      = "http://gw.api.taobao.com/router/rest";

    private void taskStart()
    {
        logger.info(" ###############start. #################### ");
        List<SellerDo> sellers = sellerDao.listActiveCompanySeller();
        if (sellers != null && !sellers.isEmpty())
        {
            for (final SellerDo sellerDo : sellers){
                if(sellerDo.getSellerType().intValue()!=SellerType.TMALL && sellerDo.getSellerType().intValue()!=SellerType.TAOBAO)
                	continue;
                
                if(getTaskToken(sellerDo.getId())){
                	logisticsExecutor.execute(new Runnable(){
						@Override
						public void run() {
							try{
								getAnomalyLogisticsTrade(sellerDo);
							}catch (Throwable e){
	                            logger.error(sellerDo.getNick()+",Error occurs when execute caculation", e);
	                        } finally{
	                            releaseTaskToken(sellerDo.getId());
	                        }
						}                		
                	});
                }else{
                	logger.info(sellerDo.getNick()+", another task is executing, SKIP");
                }
                /*
                ExecutorService executor = EXECUTOR_MAP.get(sellerDo.getDatasourceKey());
                if (executor == null)
                {
                    executor = Executors.newSingleThreadExecutor();
                    EXECUTOR_MAP.put(sellerDo.getDatasourceKey(), executor);
                }

                if (!CURRENT_STATUS.containsKey(sellerDo.getId()))
                {
                    CURRENT_STATUS.put(sellerDo.getId(), new AtomicBoolean(Boolean.TRUE));
                }

                if (CURRENT_STATUS.get(sellerDo.getId()).get())
                {
                    CURRENT_STATUS.get(sellerDo.getId()).set(Boolean.FALSE);
                    executor.execute(new Runnable()
                    {

                        @Override
                        public void run()
                        {
                            poolExecute(sellerDo);
                        }
                    });
                }
                */
            }

        }

    }

    @Override
    public void execute()
    {
        scheduledThreadPoolExecutor.scheduleWithFixedDelay(new Runnable()
        {

            @Override
            public void run()
            {
                try{
//                    if (DateUtils.nowHour() > 7 && DateUtils.nowHour() < 23)
//                    {
                    taskStart();
//                        subSeller();
//                    }
                } catch (Throwable e){
                    logger.error("XXXXXXXXXXX " + ExceptionUtils.getStackTrace(e));
                }
            }

        }, 0, 60 * 8, TimeUnit.MINUTES);

    }

    /********************************************************************************/

    private void subSeller()
    {
        List<TaobaoSubUser> subSellers = authoriztionService.listAllSubUser();
        if (subSellers != null && !subSellers.isEmpty())
        {
            for (Iterator<TaobaoSubUser> it = subSellers.iterator(); it.hasNext();)
            {
                try
                {
                    TaobaoSubUser taobaoSubUser = it.next();
                    logger.info("###########:::" + taobaoSubUser.getSubSellerNick());
                    getSubUserInfo(taobaoSubUser);
                } catch (Exception e)
                {
                    e.printStackTrace();
                    logger.error(StrUtils.showError(e));
                }
            }
        }
    }

    private void getSubUserInfo(TaobaoSubUser subUser)
    {
        TaobaoClient client = new DefaultTaobaoClient("http://gw.api.taobao.com/router/rest", "12251093",
                "96d4da9937570830d686a51de4274692");
        UserGetRequest request = new UserGetRequest();
        request.setFields("user_id,nick,seller_credit");
        UserGetResponse response = invokeSubUser(client, request, subUser.getSessionKey(), 0);
        if (response != null || response.isSuccess())
        {
            logger.info(response.getBody());
        }
    }

    private UserGetResponse invokeSubUser(TaobaoClient client, UserGetRequest request, String sessionKey, Integer num)
    {
        UserGetResponse response = null;
        try
        {
            response = client.execute(request, sessionKey);
        } catch (ApiException e)
        {
            e.printStackTrace();
            logger.error(StrUtils.showError(e));
        }
        if (response == null || !response.isSuccess())
        {
            if (++num < 3)
            {
                invokeSubUser(client, request, sessionKey, num);
            }
        }
        return response;
    }

    /********************************************************************************/
/*
    protected void poolExecute(SellerDo sellerDo)
    {

        try
        {
            logger.info(sellerDo.getNick() + "-- " + sellerDo.getId() + " 开始获取异常物流信息...");
            long start = System.currentTimeMillis();
            getAnomalyLogisticsTrade(sellerDo.getId());
            logger.info(String.format("【(%s)%s】execute anomaly logistics times(%s)-ms ", sellerDo.getNick(), sellerDo.getId(),
                    (System.currentTimeMillis() - start)));
        } catch (Exception e)
        {
            logger.error(String.format("【%s 】 ERROR : %s ", sellerDo.getNick()));
            logger.error("", e);
        }finally
        {
            CURRENT_STATUS.get(sellerDo.getId()).set(Boolean.TRUE);
        }

    }
*/

    public void getAnomalyLogisticsTrade(SellerDo seller)
    {
            List<SellerProductSubscribeDo> productList = sellerProductSubscribeDao.listBySellerId(seller.getId());
            if (productList != null && !productList.isEmpty())
            {
                Collections.sort(productList, new Comparator<SellerProductSubscribeDo>()
                {
                    @Override
                    public int compare(SellerProductSubscribeDo o1, SellerProductSubscribeDo o2)
                    {
                        return -((int) (o1.getGmtModified().getTime() - o2.getGmtModified().getTime()));
                    }
                });
            }

            AppParameterTaobao parameterTaobao = null;
            for (SellerProductSubscribeDo sellerProductSubscribeDo : productList)
            {
                if (sellerProductSubscribeDo.getProductId().intValue() == ProductType.CRM.getValue())
                {
                    parameterTaobao = new Gson().fromJson(sellerProductSubscribeDo.getParameter(), AppParameterTaobao.class);
                    break;
                }
            }
            if (parameterTaobao != null)
            {
                if (StringUtils.equalsIgnoreCase("12251093", parameterTaobao.getAppkey())
                        && StringUtils.isNotBlank(parameterTaobao.getSecret())
                        && StringUtils.isNotBlank(parameterTaobao.getAccessToken()))
                {
                    try
                    {
                        Map<String, String> ruleMap = buildAbnormalRule(seller.getId()); // 加载规则
                        if(ruleMap.size() > 0){
                            updateTradeStatusByTid(seller, parameterTaobao, ruleMap);
                        //                        synchroTrades(sellerDo, parameterTaobao, ruleMap);                        
                            synchroTradesByDB(seller, parameterTaobao, ruleMap);
                        }
                    } catch (Exception e1)
                    {
                        logger.error(String.format(" %s ERROR INFO : %s ", seller.getNick()));
                        logger.error("", e1);
                    }
                } else {
                    logger.error(String.format(" 【%s】AppParameterTaobao can't  be null. ", seller.getNick()));
                }
            }

    }

    /**
     * @param sellerDo
     * @param parameterTaobao
     * @param ruleMap
     */
    private void synchroTradesByDB(SellerDo sellerDo, AppParameterTaobao parameterTaobao, Map<String, String> ruleMap)
    {
/*        
 *      if (logger.isInfoEnabled())
        {
            logger.info(String.format(" %s ( %s ) 开始异常物流的订单数据获取", sellerDo.getNick(), sellerDo.getId()));
        }

        if (ruleMap == null || ruleMap.isEmpty())
        {
            logger.info(String.format("( %s ) 未设置异常规则 . ", sellerDo.getNick()));
            return;
        }*/

        Date startCreateTime = DateUtil.nextMonths(new Date(), -1);
        Date endCreateTime = DateUtil.nextDays(new Date(), -1);
        List<Integer> status = new ArrayList<Integer>();
        /** 5:等待买家确认收货,卖家已发货 */
        status.add(TradeStatus.WAIT_BUYER_CONFIRM_GOODS);
        int count = 5000;
        PageQuery pageQuery = new PageQuery(1, count);
        while (true)
        {
            List<TradeDo> result = tradeDao.listPageForLogistics(sellerDo.getId(), startCreateTime, endCreateTime, status, pageQuery,
                    count);
            if (result == null || result.isEmpty())
            {
                break;
            }
            /*************************************************************** 处理异常情况 */
            for (TradeDo tradeDo : result)
            {
                try {
                    AbnormalTrade searchAbnormalTrade = new AbnormalTrade();
                    searchAbnormalTrade.setSellerId(sellerDo.getId());
                    searchAbnormalTrade.setTid(Long.valueOf(tradeDo.getSourceId()));
                    AbnormalTrade existRecord = abnormalTradeService.queryAbnormalTradeByTid(searchAbnormalTrade);
                    if(existRecord != null){
                    	if (existRecord.getResetStatus() != null && existRecord.getResetStatus() == 0)
                    		continue;
                    	if(existRecord.getWuliuStatus()!=null 
                    			&& existRecord.getWuliuStatus().intValue()==AbnormalTrade.LIUZHUAN_STATUS_QSSUCCESS)
                    		continue;                   		
                    }
                	
                    AbnormalTrade abnormalTradeInfo = getAbnormalTrade(sellerDo, tradeDo);
                    if(abnormalTradeInfo.getSendProTime() == null)  //无发货时间
                    	continue; 
                    
                    Date fristStepTime = null;
                    LogisticsDetailDo detailDo = logisticsDetailDao.getLogisticsDetail(sellerDo.getId(), tradeDo.getSourceId());
                    if(detailDo != null){
                    	fristStepTime = getAbnormalTrade(detailDo, abnormalTradeInfo);
                    }
                    if(detailDo==null || fristStepTime==null){
	                    LogisticsTraceSearchResponse responseLogistics = invokLogisticsTraceSearch(sellerDo, parameterTaobao,
	                            Long.valueOf(tradeDo.getSourceId()), sellerDo.getNick(), null);
	                    if (responseLogistics == null || !responseLogistics.isSuccess())
	                        continue;	
	                    fristStepTime = getAbnormalTrade(sellerDo, responseLogistics, abnormalTradeInfo);
                    }
                    if(fristStepTime == null) //未获取到物流流转信息
                    	continue;

                    if (StringUtils.isBlank(abnormalTradeInfo.getLastTransitStepWay())){
                        abnormalTradeInfo.setLastTransitStepTime(DateUtils.now());
                        abnormalTradeInfo.setLastTransitStepWay("未获取到物流流转信息");
                        continue;
                    }
/*
                    Long sendTimes = null;
                    if (fristStepTime != null)
                    {
                        Long diff = DateUtils.now().getTime() - fristStepTime.getTime();
                        sendTimes = diff / 1000 / 60 / 60;
                    } else {                   	
                        sendTimes = 0l;
                        continue;
                    }
*/
                    // 判定是否异常物流插入 数据库  物流公司 ： 省份
                    String key = abnormalTradeInfo.getWuliuCompanyName() + ":" + abnormalTradeInfo.getReceiveProvinces();
                    String ruleTimes = compareRule(key, ruleMap);
                    if (ruleTimes == null){ //未设置异常规则
                        ruleTimes = "0";
                        continue;
                    }
                    String ruleId = ruleTimes.split("_")[0];
                    Integer ruleTime = Integer.valueOf(ruleTimes.split("_")[1]);
                    Date startTime = fristStepTime;
                    if(abnormalTradeInfo.getSendProTime().after(startTime))
                    	startTime = abnormalTradeInfo.getSendProTime();
                    Date shouldArriveTime = DateUtils.nextHours(startTime, (ruleTime*24+1));
                    abnormalTradeInfo.setShouldArriveTime(shouldArriveTime);
                    
//                    ruleTime = ruleTime * 24;
                    if (DateUtils.now().after(shouldArriveTime)) {
/*                        if (logger.isInfoEnabled())
                        {
                            logger.info(sellerDo.getNick() + " 符合规则##符合异常物流发货小时数     :  " + sendTimes + "  规则天数 :" + ruleTime
                                    + " 规则 : " + key);
                        }
 */
/*
                        TradeFullinfoGetResponse response = getTradeFullInfo(sellerDo, parameterTaobao,
                                Long.valueOf(tradeDo.getSourceId()), null);

                        if (response != null && response.isSuccess())
                        {
                            Trade tradeDetail = response.getTrade();
                            if (tradeDetail != null)
                            {
                                boolean flag = StrUtils.isEmail(tradeDetail.getBuyerEmail());
                                if (flag)
                                {
                                    abnormalTradeInfo.setEmail(tradeDetail.getBuyerEmail());
                                }
                            }
                        }
*/
                        String stepMsg = abnormalTradeInfo.getLastTransitStepWay();
                        /*if (isReceiving(stepMsg,abnormalTradeInfo.getWuliuCompanyName())) { //已签收                     
                            abnormalTradeInfo.setWuliuStatus(9);
                        } else {
                            abnormalTradeInfo.setWuliuStatus(11);
                        }*/
                        boolean isSigned = false;
                        if("SIGNED".equals(abnormalTradeInfo.getTransitStatus())||"TMS_SIGN".equals(abnormalTradeInfo.getTransitStatus()))
                        	isSigned = true;
                        if(abnormalTradeInfo.getTransitStatus()==null && isReceiving(stepMsg,abnormalTradeInfo.getWuliuCompanyName()))
                        	isSigned = true;
                        if (isSigned) { //已签收
                        	Date signTime = abnormalTradeInfo.getLastTransitStepTime();
                        	if(signTime.before(shouldArriveTime)) //签收时间未超过规定时间
                        		continue;
                        	abnormalTradeInfo.setWuliuStatus(AbnormalTrade.LIUZHUAN_STATUS_QSSUCCESS);
                        }else{
                        	abnormalTradeInfo.setWuliuStatus(AbnormalTrade.LIUZHUAN_STATUS_NOQS);
                        }
                        abnormalTradeInfo.setStatus(BaseEntity.STATUS_SUCCESS);
                        abnormalTradeInfo.setCreateTime(DateUtils.now());
                        abnormalTradeInfo.setUpdateTime(DateUtils.now());

                        
                        logger.info("[{}], found abnormal logistics trade ({}), consign time {},"
                        		+ "first logistics step info [{}], last logistics step info [{}-{}]\n"
                        		+ "expire days in rule {}, should arrive time {}, judged by DB?{}",
                        		new Object[]{sellerDo.getNick(), abnormalTradeInfo.getTid(), DateUtils.formatDate(tradeDo.getConsignTime(), "yyyy-MM-dd HH:00"),
                        		    DateUtils.formatDate(fristStepTime, "yyyy-MM-dd HH:00"), DateUtils.formatDate(abnormalTradeInfo.getLastTransitStepTime(), "yyyy-MM-dd HH:00"), stepMsg,
                        		    Integer.valueOf(ruleTime), DateUtils.formatDate(shouldArriveTime, "yyyy-MM-dd HH:00"), (abnormalTradeInfo.getTransitStatus()!=null)});

//                        AbnormalTrade oldAbnormalTrade = abnormalTradeService.queryAbnormalTradeByTid(searchAbnormalTrade);
                        if (existRecord == null)
                        {
                            if (StringUtils.equalsIgnoreCase(abnormalTradeInfo.getLastTransitStepWay(), "未获取到物流流转信息"))
                            {
                                abnormalTradeInfo.setWuliuStatus(AbnormalTrade.NO_STEP_WAY); // 无流转信息
                            }
                            abnormalTradeInfo = abnormalTradeService.insertAbnormalTrade(abnormalTradeInfo);
                        } else
                        {
                            if (!(existRecord.getResetStatus() != null && existRecord.getResetStatus() == 0))
                            { // 非手动设置为异常订单
                                if (StringUtils.equalsIgnoreCase(abnormalTradeInfo.getLastTransitStepWay(), "未获取到物流流转信息"))
                                {
                                    abnormalTradeInfo.setWuliuStatus(AbnormalTrade.NO_STEP_WAY); // 无流转信息
                                }
                                abnormalTradeInfo.setId(existRecord.getId());
                                abnormalTradeInfo.setCreateTime(null);
                                abnormalTradeService.updateAbnormalTradeById(abnormalTradeInfo);
                            }
                        }

                        //记录异常物流日志 xingxing 20150111
                        logForAbnormalTrade(abnormalTradeInfo, ruleTime, ruleId);
                    }

                } catch (Exception e)
                {
                    if (logger.isInfoEnabled())
                    {
                        logger.info(String.format("(########## %s 报错了################) \n ", StrUtils.showError(e)));
                    }                  
                }
            }
            /*************************************************************** 处理异常情况 */
            if (result.size() >= count)
            {
                pageQuery.increasePageNum();
                continue;
            } else
            {
                break;
            }
        }

    }

    /**
     * 记录并更新日志
     * 
     * @param abnormalTradeInfo
     * @param ruleTime
     * @param ruleId
     */
    private void logForAbnormalTrade(AbnormalTrade abnormalTradeInfo, Integer ruleTime, String ruleId)
    {
        List<JsonAbnormalSourceRule> sourceRules = new ArrayList<JsonAbnormalSourceRule>();

        JsonAbnormalSourceRule jsonAbnormalSourceRule = new JsonAbnormalSourceRule();
        jsonAbnormalSourceRule.setCompany(abnormalTradeInfo.getWuliuCompanyName());
        jsonAbnormalSourceRule.setProvince(abnormalTradeInfo.getReceiveProvinces());
        jsonAbnormalSourceRule.setOverTime(ruleTime.toString());
        jsonAbnormalSourceRule.setRuleId(ruleId);
        jsonAbnormalSourceRule.setCreateTime(DateUtils.formatDate(new Date(), "yyyy-MM-dd HH:mm:ss"));

        SellerAbnormalTradeLogDo sellerAbnormalTradeLogDo = new SellerAbnormalTradeLogDo();
        sellerAbnormalTradeLogDo.setSellerId(abnormalTradeInfo.getSellerId());
        sellerAbnormalTradeLogDo.setAbnormalId(abnormalTradeInfo.getId());
        sellerAbnormalTradeLogDo.setTid(abnormalTradeInfo.getTid());
        sellerAbnormalTradeLogDo.setStatus(0);

        List<Long> abnormals = new ArrayList<Long>();
        abnormals.add(abnormalTradeInfo.getId());
        List<SellerAbnormalTradeLogDo> oldAbnormalTradeLogDos = this.sellerAbnormalTradeLogDao.loadByAbnormalTradeLogDo(
                abnormalTradeInfo.getSellerId(), abnormals);

        if (oldAbnormalTradeLogDos == null || oldAbnormalTradeLogDos.isEmpty())
        {
            sourceRules.add(jsonAbnormalSourceRule);
            sellerAbnormalTradeLogDo.setSourceRule(JsonAbnormalSourceRule.abnormalSourceRulesToStr(sourceRules));
            this.sellerAbnormalTradeLogDao.create(sellerAbnormalTradeLogDo);
        } else
        {
            sourceRules = oldAbnormalTradeLogDos.get(0).getJsonAbnormalSourceRules();
            sourceRules.add(jsonAbnormalSourceRule);
            sellerAbnormalTradeLogDo.setSourceRule(JsonAbnormalSourceRule.abnormalSourceRulesToStr(sourceRules));
            this.sellerAbnormalTradeLogDao.update(sellerAbnormalTradeLogDo);
        }
        logger.info("[{}-{}], 记录异常物流日志，Rule：{}", new Object[]{abnormalTradeInfo.getSellerId(), abnormalTradeInfo.getTid(), 
        		sellerAbnormalTradeLogDo.getSourceRule()});
    }

    /**
     * @param sellerDo
     * @param parameterTaobao
     * @param set
     * @throws Exception
     * @throws Exception
     */
    private void synchroTrades(SellerDo sellerDo, AppParameterTaobao parameterTaobao, Map<String, String> ruleMap) throws Exception
    {

        if (logger.isInfoEnabled())
        {
            logger.info(String.format(" %s ( %s ) 开始异常物流的订单数据获取", sellerDo.getNick(), sellerDo.getId()));
        }

        if (ruleMap == null || ruleMap.isEmpty())
        {
            logger.info(String.format("( %s ) 未设置异常规则 . ", sellerDo.getNick()));
            return;
        }
        Long pageNum = 1L;
        Long pageSize = 80L;
        Long totalPage = 0L;

        TradesSoldGetRequest req = buildTradeRequest(pageNum, pageSize, ruleMap);
        TradesSoldGetResponse responseTrades = executeTradeSold(parameterTaobao, req, null);

        if (responseTrades != null && responseTrades.isSuccess())
        {
            totalPage = getTotalPage(responseTrades, pageSize);
        } else
        {
            logger.error(String.format("( %s )接口调用失败. ", sellerDo.getNick()));
        }

        while (!(pageNum > totalPage))
        {

            if (responseTrades != null && responseTrades.isSuccess())
            {

                List<Trade> list = responseTrades.getTrades();
                for (Trade t : list)
                {
                    AbnormalTrade abnormalTradeInfo = getAbnormalTrade(sellerDo, t);

                    LogisticsTraceSearchResponse responseLogistics = invokLogisticsTraceSearch(sellerDo, parameterTaobao, t.getTid(),
                            t.getSellerNick(), null);
                    if (responseLogistics == null || !responseLogistics.isSuccess())
                    {
                        continue;
                    }

                    Date fristStepTime = getAbnormalTrade(sellerDo, responseLogistics, abnormalTradeInfo);

                    if (StringUtils.isBlank(abnormalTradeInfo.getLastTransitStepWay()))
                    {
                        abnormalTradeInfo.setLastTransitStepTime(DateUtils.now());
                        abnormalTradeInfo.setLastTransitStepWay("未获取到物流流转信息");
                    }

                    //                  物流 时间差，比较异常物流
                    Long sendTimes = null;

                    //                    if (abnormalTradeInfo.getSendProTime() != null) {
                    //                        sendTimes = DateUtils.diffDays(DateUtils.now(),
                    //                                abnormalTradeInfo.getSendProTime());// 卖家发货时间
                    //                    }

                    //                    sendTimes = DateUtils.getDays(fristStepTime, DateUtils.now()); //第一条流转信息
                    //                    if (sendTimes == null || sendTimes < 0) {
                    //                        sendTimes = 0;
                    //                    }

                    if (fristStepTime != null)
                    {
                        Long diff = DateUtils.now().getTime() - fristStepTime.getTime();
                        sendTimes = diff / 1000 / 60 / 60;
                    } else
                    {
                        sendTimes = 0l;
                    }

                    // 判定是否异常物流插入 数据库  物流公司 ： 省份
                    String key = abnormalTradeInfo.getWuliuCompanyName() + ":" + abnormalTradeInfo.getReceiveProvinces();
                    String ruleTimes = compareRule(key, ruleMap);

                    if (ruleTimes == null)
                    {
                        ruleTimes = "0";
                        continue;
                    }
                    Integer ruleTime = Integer.valueOf(ruleTimes.split("_")[1]);
                    //zmn:计算应该到达的时间(发货时间,规则中的天数)
                    Date shouldArriveTime = DateUtils.nextDays(abnormalTradeInfo.getSendProTime(), ruleTime);
                    //zmn:set应该到达的时间
                    abnormalTradeInfo.setShouldArriveTime(shouldArriveTime);
                    
                    String ruleId = ruleTimes.split("_")[0];
                    ruleTime = ruleTime * 24;

                    // 符合异常物流规则
                    if (ruleTime.intValue() < sendTimes.intValue())
                    {

                        if (logger.isInfoEnabled())
                        {
                            logger.info(sellerDo.getNick() + " 符合规则##符合异常物流发货小时数     :  " + sendTimes + "  规则天数 :" + ruleTime
                                    + " 规则 : " + key);
                        }

                        TradeFullinfoGetResponse response = getTradeFullInfo(sellerDo, parameterTaobao, t.getTid(), null);

                        if (response != null && response.isSuccess())
                        {
                            Trade tradeDetail = response.getTrade();
                            if (tradeDetail != null)
                            {
                                boolean flag = StrUtils.isEmail(tradeDetail.getBuyerEmail());
                                if (flag)
                                {
                                    abnormalTradeInfo.setEmail(tradeDetail.getBuyerEmail());
                                }
                            }
                        }

                        String stepMsg = abnormalTradeInfo.getLastTransitStepWay();

                        if (StrUtils.isNotEmpty(stepMsg))
                        {
                            if (isReceiving(stepMsg,abnormalTradeInfo.getWuliuCompanyName()))
                            {
                                abnormalTradeInfo.setWuliuStatus(9);
                            } else
                            {
                                abnormalTradeInfo.setWuliuStatus(11);
                            }
                        }
                        abnormalTradeInfo.setStatus(BaseEntity.STATUS_SUCCESS);
                        abnormalTradeInfo.setCreateTime(DateUtils.now());
                        abnormalTradeInfo.setUpdateTime(DateUtils.now());

                        AbnormalTrade searchAbnormalTrade = new AbnormalTrade();
                        searchAbnormalTrade.setSellerId(abnormalTradeInfo.getSellerId());
                        searchAbnormalTrade.setTid(abnormalTradeInfo.getTid());

                        AbnormalTrade oldAbnormalTrade = abnormalTradeService.queryAbnormalTradeByTid(searchAbnormalTrade);
                        if (oldAbnormalTrade == null)
                        {
                            if (StringUtils.equalsIgnoreCase(abnormalTradeInfo.getLastTransitStepWay(), "未获取到物流流转信息"))
                            {
                                abnormalTradeInfo.setWuliuStatus(AbnormalTrade.NO_STEP_WAY); // 无流转信息
                            }
                            abnormalTradeService.insertAbnormalTrade(abnormalTradeInfo);
                        } else
                        {
                            if (!(oldAbnormalTrade.getResetStatus() != null && oldAbnormalTrade.getResetStatus() == 0))
                            { // 非手动设置为异常订单
                                if (StringUtils.equalsIgnoreCase(abnormalTradeInfo.getLastTransitStepWay(), "未获取到物流流转信息"))
                                {
                                    abnormalTradeInfo.setWuliuStatus(AbnormalTrade.NO_STEP_WAY); // 无流转信息
                                }
                                abnormalTradeInfo.setId(oldAbnormalTrade.getId());
                                abnormalTradeInfo.setCreateTime(null);
                                abnormalTradeService.updateAbnormalTradeById(abnormalTradeInfo);
                            }
                        }
                        if (logger.isInfoEnabled())
                        {
                            logger.info(String.format("(########## %s 更新异常物流信息 tid ( %s)) \n ", sellerDo.getNick(),
                                    abnormalTradeInfo.getTid()));
                        }
                    } else
                    {

                        //                        if (logger.isInfoEnabled()) {
                        //                            logger.info(sellerDo.getNick() + " 不符合规则==物流发货到系统当前时间 :  " + sendTimes
                        //                                    + " 规则天数 :" + ruleTime + " 规则 : " + key);
                        //                        }

                    }
                }
            }
            if (!(pageNum > totalPage))
            {
                req.setPageNo(++pageNum);
                responseTrades = executeTradeSold(parameterTaobao, req, null);
            }
        }
    }

    /**
     * 先查推送库
     * 
     * @param sellerDo
     * @param parameterTaobao
     * @param t
     * @param flag
     * @return
     */
    private TradeFullinfoGetResponse getTradeFullInfo(SellerDo sellerDo, AppParameterTaobao parameterTaobao, Long tid, Integer flag)
    {
        if (flag == null)
        {
            flag = 0;
        }
        TradeFullinfoGetRequest reqtrade = new TradeFullinfoGetRequest();
        reqtrade.setTid(tid);
        reqtrade.setFields("buyer_nick,buyer_email");
        TradeFullinfoGetResponse fullInfo = null;
        try
        {
            if (flag > 0)
            {
                TimeUnit.MILLISECONDS.sleep(500 * 2 * flag);
            }
            Trade trade = apiService.loadTarde(sellerDo.getNick(), reqtrade.getTid());
            if (trade == null)
            {
                fullInfo = getTaobaoClient(parameterTaobao).execute(reqtrade, parameterTaobao.getAccessToken());
            } else
            {
                fullInfo = new TradeFullinfoGetResponse();
                fullInfo.setTrade(trade);
            }
        } catch (Exception e)
        {
            e.printStackTrace();
            logger.error(String.format("getTradeFullInfo error ( %s )", Arrays.toString(e.getStackTrace())));
        }
        if (fullInfo == null || !fullInfo.isSuccess())
        {
            if (flag <= 3)
            {
                getTradeFullInfo(sellerDo, parameterTaobao, tid, ++flag);
            }
        }
        return fullInfo;
    }

    @SuppressWarnings("rawtypes")
    private Date getAbnormalTrade(SellerDo sellerDo, LogisticsTraceSearchResponse response, AbnormalTrade abnormalTradeInfo)
            throws ParseException
    {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date fristStepTime = null;
        if (response != null && response.isSuccess())
        {
            List list = response.getTraceList();
            if (list != null && !list.isEmpty())
            {
                TransitStepInfo fristStepInfo = (TransitStepInfo) list.get(0);
                if (fristStepInfo != null && fristStepInfo.getStatusTime() != null)
                {
                    fristStepTime = sdf.parse(fristStepInfo.getStatusTime());
                }
                int lastIndex = list.size() - 1;
                TransitStepInfo lastStepInfo = (TransitStepInfo) list.get(lastIndex);
                if (lastStepInfo != null)
                {
                    if (StrUtils.isNotEmpty(lastStepInfo.getStatusTime()))
                    {
                        abnormalTradeInfo.setLastTransitStepTime(sdf.parse(lastStepInfo.getStatusTime()));
                        abnormalTradeInfo.setLastTransitStepWay(lastStepInfo.getStatusDesc());
                        abnormalTradeInfo.setReceivingTime(sdf.parse(lastStepInfo.getStatusTime()));
                    } else
                    {
                        abnormalTradeInfo.setLastTransitStepWay(null);
                    }
                    abnormalTradeInfo.setOutSid(response.getOutSid());
                    //                                     物流公司名称
                    abnormalTradeInfo.setWuliuCompanyName(response.getCompanyName());
                    //                                   淘宝返回有问题          
                    //                                   abnormalTradeInf.setTransitStatus(responseLogistics.getStatus());    

                }
            }
        } else
        {
            if (logger.isInfoEnabled())
            {
                logger.info(String.format("( %s ) 未获取到接口物流数据  . ", sellerDo.getNick()));
            }
        }
        return fristStepTime;
    }
    
    private Date getAbnormalTrade(LogisticsDetailDo logisticsDetail, AbnormalTrade abnormalTradeInfo){
    	String stepDetail = logisticsDetail.getLogisticsTrace();
		List<LogisticsDetailTrace> stepList = JSON.parseArray(stepDetail, LogisticsDetailTrace.class);
		Date firstStepTime = null;
		abnormalTradeInfo.setLastTransitStepTime(logisticsDetail.getLastStepTime());
        abnormalTradeInfo.setLastTransitStepWay(stepList.get(stepList.size()-1).getDesc());
        abnormalTradeInfo.setReceivingTime(logisticsDetail.getLastStepTime());
        abnormalTradeInfo.setOutSid(logisticsDetail.getExpressNo());
        abnormalTradeInfo.setWuliuCompanyName(logisticsDetail.getCompanyName());
        for(LogisticsDetailTrace trace : stepList){
        	if(trace.getAction().equals("SIGNED") || trace.getAction().equals("TMS_SIGN"))
        		abnormalTradeInfo.setTransitStatus(trace.getAction());
        	else if(trace.getAction().equals("GOT") || trace.getAction().equals("TMS_ACCEPT")) //揽件成功
        		firstStepTime = trace.getTime();
        }
        if(abnormalTradeInfo.getTransitStatus() == null)
        	abnormalTradeInfo.setTransitStatus(stepList.get(stepList.size()-1).getAction());
        
        return firstStepTime;
    }

    private AbnormalTrade getAbnormalTrade(SellerDo sellerDo, TradeDo tradeDo)
    {
        AbnormalTrade abnormalTradeInfo = new AbnormalTrade();
        //      收货省份
        abnormalTradeInfo.setReceiveProvinces(tradeDo.getReceiverState());
        //      物流公司名称
        //      abnormalTradeInf.setWuliuCompanyName("");
        //      卖家发货时间
        abnormalTradeInfo.setSendProTime(tradeDo.getConsignTime());
        //      最后物流流转信息
        //      abnormalTradeInf.setLastTransitStepWay("");
        abnormalTradeInfo.setTid(Long.valueOf(tradeDo.getSourceId()));
        abnormalTradeInfo.setSellerId(sellerDo.getId());
        abnormalTradeInfo.setSellerNick(sellerDo.getNick());
        abnormalTradeInfo.setBuyerNick(tradeDo.getBuyerNick());
        abnormalTradeInfo.setTradeCreated(tradeDo.getCreated());
        abnormalTradeInfo.setReceiverName(tradeDo.getReceiverName());
        abnormalTradeInfo.setPayTime(tradeDo.getPayTime());
        abnormalTradeInfo.setUpdateTime(new Date());
        if (StrUtils.isNotEmpty(tradeDo.getReceiverMobile()))
            abnormalTradeInfo.setMobileNumber(tradeDo.getReceiverMobile());
        if (StrUtils.isEmail(tradeDo.getBuyerEmail()))
        	abnormalTradeInfo.setEmail(tradeDo.getBuyerEmail());
        	
        return abnormalTradeInfo;
    }

    private AbnormalTrade getAbnormalTrade(SellerDo sellerDo, Trade t)
    {
        AbnormalTrade abnormalTradeInfo = new AbnormalTrade();
        //      收货省份
        abnormalTradeInfo.setReceiveProvinces(t.getReceiverState());
        //      物流公司名称
        //      abnormalTradeInf.setWuliuCompanyName("");
        //      卖家发货时间
        abnormalTradeInfo.setSendProTime(t.getConsignTime());
        //      最后物流流转信息
        //      abnormalTradeInf.setLastTransitStepWay("");
        abnormalTradeInfo.setTid(t.getTid());
        abnormalTradeInfo.setSellerId(sellerDo.getId());
        abnormalTradeInfo.setSellerNick(t.getSellerNick());

        abnormalTradeInfo.setBuyerNick(t.getBuyerNick());
        abnormalTradeInfo.setTradeCreated(t.getCreated());
        abnormalTradeInfo.setReceiverName(t.getReceiverName());
        abnormalTradeInfo.setPayTime(t.getPayTime());
        abnormalTradeInfo.setUpdateTime(new Date());
        if (StrUtils.isNotEmpty(t.getReceiverMobile()))
        {
            abnormalTradeInfo.setMobileNumber(t.getReceiverMobile());
        }
        return abnormalTradeInfo;
    }

    private TradesSoldGetResponse executeTradeSold(AppParameterTaobao parameterTaobao, TradesSoldGetRequest req, Integer flag)
    {

        if (flag == null)
        {
            flag = 0;
        }
        TradesSoldGetResponse responseTrades = null;
        try
        {
            if (flag > 0)
            {
                TimeUnit.MILLISECONDS.sleep(500 * 2 * flag);
            }

            responseTrades = getTaobaoClient(parameterTaobao).execute(req, parameterTaobao.getAccessToken());
        } catch (Exception e)
        {
            e.printStackTrace();
            logger.error(String.format("executeTradeSold error : ", Arrays.toString(e.getStackTrace())));
        }
        if (responseTrades == null || !responseTrades.isSuccess())
        {
            if (flag <= 3)
            {
                executeTradeSold(parameterTaobao, req, ++flag);
            }
        }
        return responseTrades;
    }

    private TradesSoldGetRequest buildTradeRequest(Long pageNum, Long pageSize, Map<String, String> ruleMap)
    {

        TradesSoldGetRequest request = new TradesSoldGetRequest();
        Integer endTimes = getMinTimes(ruleMap);
        //        Integer endTimes = getMaxTimes(ruleMap);
        request.setFields("tid,seller_nick,buyer_nick,title,type,created,seller_rate,buyer_flag,buyer_rate,status,payment,adjust_fee,post_fee,total_fee,pay_time,end_time,modified,consign_time,buyer_obtain_point_fee,point_fee,real_point_fee,received_payment,commission_fee,buyer_memo,seller_memo,alipay_id,alipay_no,buyer_message,pic_path,num_iid,num,price,buyer_alipay_no,receiver_name,receiver_state,receiver_city,receiver_district,receiver_address,receiver_zip,receiver_mobile,receiver_phone,buyer_email,seller_flag,seller_alipay_no,seller_mobile,seller_phone,seller_name,seller_email,available_confirm_fee,has_post_fee,timeout_action_time,snapshot_url,cod_fee,cod_status,shipping_type,trade_memo,is_3D,orders,service_orders,promotion_details,invoice_name,buyer_cod_fee,seller_cod_fee,express_agency_fee,discount_fee,trade_from,is_force_wlb,is_brand_sale,is_lgtype,step_trade_status,step_paid_fee");

        Date searchStartDate = DateUtil.nextMonths(new Date(), -1);
        Date searchEndDate = DateUtil.nextDays(new Date(), -1);

        //        Date searchEndDate = DateUtil.nextDays(new Date(), -endTimes);

        //        Date searchStartDate = DateUtil.nextDays(new Date(), -endTimes);
        //        Date searchEndDate = DateUtil.nextDays(new Date(), -1);

        request.setStartCreated(searchStartDate);
        request.setEndCreated(searchEndDate);
        request.setStatus("WAIT_BUYER_CONFIRM_GOODS");
        request.setPageNo(pageNum);
        request.setPageSize(pageSize);
        return request;

    }

    private Integer getMinTimes(Map<String, String> map)
    {
        if (map == null || map.isEmpty())
        {
            throw new IllegalArgumentException(" rule map can't be null . ");
        }
        Integer i = null;
        Set<Map.Entry<String, String>> set = map.entrySet();
        Iterator<Entry<String, String>> it = set.iterator();
        while (it.hasNext())
        {
            Map.Entry<java.lang.String, java.lang.String> entry = (Map.Entry<java.lang.String, java.lang.String>) it.next();
            if (i == null)
            {
                i = Integer.valueOf(entry.getValue().split("_")[1]);
            }
            if (Integer.valueOf(entry.getValue().split("_")[1]) < i)
            {
                i = Integer.valueOf(entry.getValue().split("_")[1]);
            }
        }
        if (i == null)
        {
            i = 1;
        }
        return i;
    }

    /**
     * @param map
     * @return
     */
    private Integer getMaxTimes(Map<String, Integer> map)
    {
        if (map == null || map.isEmpty())
        {
            throw new IllegalArgumentException(" rule map can't be null . ");
        }
        Integer i = null;
        Set<Map.Entry<String, Integer>> set = map.entrySet();
        Iterator<Entry<String, Integer>> it = set.iterator();
        while (it.hasNext())
        {
            Map.Entry<java.lang.String, java.lang.Integer> entry = (Map.Entry<java.lang.String, java.lang.Integer>) it.next();
            if (i == null)
            {
                i = entry.getValue();
            }
            if (entry.getValue() > i)
            {
                i = entry.getValue();
            }
        }
        if (i == null)
        {
            i = 5;
        } else
        {
            i += 5;
        }
        return i;
    }

    private LogisticsTraceSearchResponse invokLogisticsTraceSearch(SellerDo sellerDo, AppParameterTaobao parameterTaobao, Long tid,
                                                                   String sellerNick, Integer flag)
    {
        if (flag == null)
        {
            flag = 0;
        }
        LogisticsTraceSearchRequest reqLogistics = new LogisticsTraceSearchRequest();
        reqLogistics.setTid(tid);
        reqLogistics.setSellerNick(sellerNick);
        LogisticsTraceSearchResponse response = null;
        try
        {
            if (flag > 0)
            {
                TimeUnit.MILLISECONDS.sleep(500 * 2 * flag);
            }
            response = getTaobaoClient(parameterTaobao).execute(reqLogistics, parameterTaobao.getAccessToken());
        } catch (Exception e)
        {
//            e.printStackTrace();
            logger.error(String.format("( %s ) invokLogisticsTraceSearch error : %s ", sellerDo.getNick()));
            logger.error("", e);
        }
        if (response == null || !response.isSuccess())
        {
            if (flag < 4){
            	String errorCode = response.getErrorCode();
            	String subCode = response.getSubCode();
            	if(!"27".equals(errorCode) && !"isv.split-data-error:CD24".equals(subCode)) {
                    invokLogisticsTraceSearch(sellerDo, parameterTaobao, tid, sellerNick, ++flag);
            	}
            }
        }
        return response;
    }

    private String compareRule(String key, Map<String, String> ruleMap)
    {
        if (ruleMap != null)
            if (ruleMap.containsKey(key))
            {
                return ruleMap.get(key);
            }
        return null;
    }

    /**
     * 是否已经签收
     * 
     * @param stepMsg
     * @return
     */
    private boolean isReceiving(String stepMsg,String companyName)
    {

    	if (StringUtils.isBlank(stepMsg))
        {
            return Boolean.FALSE;
        } else if (stepMsg.contains("已签")){
            return Boolean.TRUE;
        } else if (stepMsg.contains("签收人")){
            return Boolean.TRUE;
        } else if (stepMsg.contains("派送成功")){
            return Boolean.TRUE;
        } else if (stepMsg.contains("已被")){
            return Boolean.TRUE;
        } else if (stepMsg.contains("签收")){
        	return Boolean.TRUE;
        } else if (stepMsg.contains("草签")){
        	return Boolean.TRUE;
        } else if (stepMsg.contains("已妥投")){
            return Boolean.TRUE;
        } else if (stepMsg.contains("已被签收")){
            return Boolean.TRUE;
        } else if (stepMsg.contains("完成取件")){
        	return Boolean.TRUE;
        } else if (stepMsg.contains("代收")) {
        	return Boolean.TRUE;
        }
        return Boolean.FALSE;
    	/*
    	if (StringUtils.isBlank(desc))
        {
            return Boolean.FALSE;
        }
        if (StringUtils.equalsIgnoreCase(companyName, "顺丰速运") || StringUtils.equalsIgnoreCase(companyName, "圆通速递")
                || StringUtils.equalsIgnoreCase(companyName, "全峰快递"))
        {
            return desc.contains("已签收");
        } else if (StringUtils.equalsIgnoreCase(companyName, "申通快递"))
        {
            return desc.contains("草签") || desc.contains("已签收") || desc.contains("已签");
        } else if (StringUtils.equalsIgnoreCase(companyName, "韵达快递"))
        {
            return desc.contains("已被签收") || desc.contains("已签收");
        } else if (StringUtils.equalsIgnoreCase(companyName, "中通快递"))
        {
            return desc.contains("拍照签收") || desc.contains("已签收") || desc.contains("已签");
        } else if (StringUtils.equalsIgnoreCase(companyName, "EMS"))
        {
            return desc.contains("已妥投");
        } else
        {
            return desc.contains("已签收");
        }
        */
    }

    private Long getTotalPage(TradesSoldGetResponse responseTrades, Long pageSize)
    {
        Long totalPage = 0l;
        if (responseTrades.getTotalResults() != null && responseTrades.getTotalResults() > 0)
        {
            totalPage = responseTrades.getTotalResults() / pageSize;
            if (responseTrades.getTotalResults() % pageSize > 0)
            {
                totalPage++;
            }
        }
        return totalPage;
    }

    /**
     * 更新数据库已经同步下来的订单的状态（判断交易是否成功）
     * 
     * @param ruleMap
     * @throws Exception
     */
    @SuppressWarnings("rawtypes")
    private void updateTradeStatusByTid(SellerDo sellerDo, AppParameterTaobao parameterTaobao, Map<String, String> ruleMap)
            throws Exception
    {

        if (ruleMap == null || ruleMap.isEmpty())
        { // 当卖家没有任何规则删除所有异常物流数据
//            deleteAllAbnormalTrades(sellerDo.getId());
            return;
        }

        List<Long> deleteId = new ArrayList<Long>();
/*        TradeGetRequest req = new TradeGetRequest();
        req.setFields("tid,status,seller_nick,buyer_nick");
 */

        PageQuery pageQuery = new PageQuery(0, 5000);
        Date startTime = DateUtils.nextDays(DateUtils.now(), -31);
        while (true)
        {
            PageList<AbnormalTradeDo> result = dolphinAbnormalTradeDao.listByPage(sellerDo.getId(), startTime, null, pageQuery);
            if (result != null && !result.isEmpty())
            {
                Iterator<AbnormalTradeDo> it = result.iterator();
                while (it.hasNext())
                {
                	AbnormalTradeDo abnormal = (AbnormalTradeDo) it.next();

                    if (abnormal.getResetStatus() != null && abnormal.getResetStatus().intValue() == 0)
                    { // 手动调整为非异常订单,不做任务处理
                        continue;
                    }

                    if (abnormal.getTid() == null || abnormal.getWuliuStatus() == null
                            || !StrUtils.isNotEmpty(abnormal.getWuliuCompanyName())
                            || !StrUtils.isNotEmpty(abnormal.getReceiveProvinces()))
                    {
                        deleteId.add(abnormal.getId());
                        continue;
                    }

                    String key = abnormal.getWuliuCompanyName() + ":" + abnormal.getReceiveProvinces();
                    String day = compareRule(key, ruleMap);
                    if (day == null)  //异常物流规则已被删除
                    {
//                        deleteId.add(abnormal.getId());
                        continue;
                    }
                    //                    logger.info(String.format("key : ( %s ) day : ( %s ) ", key, day));
                    if (abnormal.getWuliuStatus().intValue() == 9)  //已签收
                        continue;                   
                    
                    TradeDo trade  = tradeDao.findBySourceId(sellerDo.getId(), abnormal.getTid().toString());
                    if(trade == null || trade.getStatus()==null)
                    	continue;
                    if(trade.getStatus().intValue() == TradeStatus.TRADE_FINISHED){
                    	abnormal.setWuliuStatus(9);
                    	AbnormalTradeDo newRecord = new AbnormalTradeDo();
                    	newRecord.setId(abnormal.getId());
                    	newRecord.setSellerId(sellerDo.getId());
                    	newRecord.setWuliuStatus(AbnormalTrade.LIUZHUAN_STATUS_QSSUCCESS);
                    	dolphinAbnormalTradeDao.update(newRecord);
                    }else if(trade.getStatus().intValue() == TradeStatus.WAIT_BUYER_CONFIRM_GOODS){
                    	boolean isSigned = false;
                    	LogisticsDetailDo detailDo = logisticsDetailDao.getLogisticsDetail(sellerDo.getId(), abnormal.getTid().toString());
                    	if(detailDo != null){
                    		String stepDetail = detailDo.getLogisticsTrace();
                    		List<LogisticsDetailTrace> stepList = JSON.parseArray(stepDetail, LogisticsDetailTrace.class);
                    		for(LogisticsDetailTrace trace : stepList){
                    			if("SIGNED".equals(trace.getAction())||"TMS_SIGN".equals(trace.getAction())){
                    				isSigned = true;
                    				break;
                    			}
                    		}
                    	}else{
	                    	LogisticsTraceSearchResponse responseLogistics = invokLogisticsTraceSearch(sellerDo, parameterTaobao,
	                    			Long.valueOf(trade.getSourceId()), sellerDo.getNick(), null);
	                        if (responseLogistics != null && responseLogistics.isSuccess()) {
	                            List logisticsList = responseLogistics.getTraceList();
	                            if (logisticsList != null && !logisticsList.isEmpty()){
	                                int lastIndex = logisticsList.size() - 1;
	                                TransitStepInfo stepInfo = (TransitStepInfo) logisticsList.get(lastIndex);
	                                if (stepInfo != null && StringUtils.isNotBlank(stepInfo.getStatusDesc())){
	                                    String desc = stepInfo.getStatusDesc();
	                                    if (isReceiving(desc,abnormal.getWuliuCompanyName())) 
	                                    	isSigned = true;
	                                }
	                            }
	                        }
                    	}
                    	if(isSigned){
                    		abnormal.setWuliuStatus(9);
                            AbnormalTradeDo newRecord = new AbnormalTradeDo();
                        	newRecord.setId(abnormal.getId());
                        	newRecord.setSellerId(sellerDo.getId());
                        	newRecord.setWuliuStatus(AbnormalTrade.LIUZHUAN_STATUS_QSSUCCESS);
                        	dolphinAbnormalTradeDao.update(newRecord);
                    	}
                    }
                    	
//                    req.setTid(abnormal.getTid());
//                    TradeGetResponse response = getTradeResponse(abnormal.getSellerNick(), parameterTaobao, req, null);
                    /*
                    if (response != null && response.isSuccess())
                    {
                        Trade t = response.getTrade();
                        if (t != null)
                        {
                            //若果交易成功
                            if ("TRADE_FINISHED".equals(t.getStatus()))
                            {
                                abnormal.setWuliuStatus(9);
                                abnormalTradeService.updateAbnormalTradeById(abnormal);
                                continue;
                            } else if (StringUtils.equalsIgnoreCase("WAIT_BUYER_CONFIRM_GOODS", t.getStatus()))
                            {
                                LogisticsTraceSearchResponse responseLogistics = invokLogisticsTraceSearch(sellerDo, parameterTaobao,
                                        t.getTid(), t.getSellerNick(), null);

                                if (responseLogistics != null && responseLogistics.isSuccess())
                                {
                                    List logisticsList = responseLogistics.getTraceList();
                                    if (logisticsList != null && !logisticsList.isEmpty())
                                    {
                                        int lastIndex = logisticsList.size() - 1;
                                        TransitStepInfo stepInfo = (TransitStepInfo) logisticsList.get(lastIndex);
                                        if (stepInfo != null && StringUtils.isNotBlank(stepInfo.getStatusDesc()))
                                        {
                                            String desc = stepInfo.getStatusDesc();
                                            if (isReceiving(desc,abnormal.getWuliuCompanyName()))
                                            {
                                                abnormal.setWuliuStatus(9);
                                                abnormalTradeService.updateAbnormalTradeById(abnormal);
                                            }
                                        }
                                    }
                                }
                                continue;
                            }
                        } else
                        {
                            deleteId.add(abnormal.getId());
                            continue;
                        }
                    } else
                    {
                        deleteId.add(abnormal.getId());
                        continue;
                    }
                    */
                }
            } else
            {
                break;
            }
            Paginator paginator = result.getPaginator();
            if (paginator.getNextPage() == paginator.getPage())
            {
                break;
            }
            pageQuery.increasePageNum();
        }

        if (deleteId != null && !deleteId.isEmpty())
        {
            deleteAbnormalTrade(sellerDo, deleteId);
        }
    }

    /**
     * 先查推送库的订单
     * 
     * @param sellerNick
     * @param parameterTaobao
     * @param req
     * @param flag
     * @return
     */
    private TradeGetResponse getTradeResponse(String sellerNick, AppParameterTaobao parameterTaobao, TradeGetRequest req, Integer flag)
    {

        TradeGetResponse response = null;
        if (flag == null)
        {
            flag = 0;
        }
        try
        {
            if (flag > 0)
            {
                TimeUnit.MILLISECONDS.sleep(500 * 2 * flag);
            }
            Trade trade = apiService.loadTarde(sellerNick, req.getTid());
            if (trade == null)
            {
                response = getTaobaoClient(parameterTaobao).execute(req, parameterTaobao.getAccessToken());
            } else
            {
                response = new TradeGetResponse();
                response.setTrade(trade);
            }
        } catch (Exception e)
        {
            e.printStackTrace();
            logger.error(Arrays.toString(e.getStackTrace()));
        }
        if (response == null || !response.isSuccess())
        {
            if (flag <= 3 && !StringUtils.equalsIgnoreCase(response.getErrorCode(), "27"))
            {
                getTradeResponse(sellerNick, parameterTaobao, req, ++flag);
            }
        }
        return response;
    }

    /**
     * key:快递公司:省份,  e.g.韵达快递:上海
     * value:ruleId_time e.g. 2418329293_7
     */
    private Map<String, String> buildAbnormalRule(Long sellerId)
    {
        Long start = System.currentTimeMillis();
//        logger.info(sellerId + " 开始加载卖家异常物流规则 ");

        Map<String, String> rule = new HashMap<String, String>();
        SellerAbnormalTradeRuleDo searchAbnormalTradeRuleDo = new SellerAbnormalTradeRuleDo();
        searchAbnormalTradeRuleDo.setSellerId(sellerId);
        searchAbnormalTradeRuleDo.setStatus(BaseType.DELETED_NO);
        List<SellerAbnormalTradeRuleDo> ruleList = sellerAbnormalTradeRuleDao.loadAbnormalTradeRuleList(searchAbnormalTradeRuleDo);
        for (SellerAbnormalTradeRuleDo sellerAbnormalTradeRuleDo : ruleList){
            JsonObject json = new JsonParser().parse(sellerAbnormalTradeRuleDo.getJsonStr()).getAsJsonObject();
            StringBuilder sb = new StringBuilder();
            if (json.get("time") != null)
            {
                Integer time = Integer.parseInt((json.get("time").toString()).replace("\"", ""));
                if (time == null)
                {
                    continue;
                }
                if (json.get("companies") != null)
                {
                    String[] companies = (json.get("companies").toString()).split(" ");

                    for (String str : companies)
                    {
                        if (json.get("provinces") != null)
                        {
                            String[] provinces = (json.get("provinces").toString()).split(" ");
                            for (String p : provinces)
                            {
                                sb.setLength(0);
                                sb.append(str).append(":");
                                sb.append(p);
                                String key = (sb.toString()).replaceAll("\"", "");
                                if (rule.containsKey(key))
                                {
                                    Integer oldTime = Integer.valueOf(rule.get(key).split("_")[1]);
                                    if (oldTime > time)
                                    {
                                        rule.put(key, sellerAbnormalTradeRuleDo.getId() + "_" + time);
                                    }
                                } else
                                {
                                    rule.put(key, sellerAbnormalTradeRuleDo.getId() + "_" + time);
                                }
                            }
                        }
                    }
                }
            }
        }
        if(rule.size() > 0){
            logger.info(String.format("【%s】规则加载完成,共(%s)条 time(%s)-ms", sellerId, rule.size(), (System.currentTimeMillis() - start)));
        }
        
        return rule;
    }

    private void deleteAbnormalTrade(SellerDo sellerDo, List<Long> deleteId)
    {
        abnormalTradeDao.deleteAbnormalTrades(sellerDo, deleteId);
        if (logger.isInfoEnabled())
        {
            logger.info(String.format("( %s ) remove abnormal trades size ( %s ) .", sellerDo.getNick(), deleteId.size()));
        }
    }

    private void deleteAllAbnormalTrades(Long id)
    {
        SellerDo sellerDo = new SellerDo();
        sellerDo.setId(id);
        abnormalTradeDao.deleteAbnormalTrades(sellerDo, null);
        if (logger.isInfoEnabled())
        {
            logger.info(String.format("( %s ) remove all abnormal trades .", id));
        }
    }

    private TaobaoClient getTaobaoClient(AppParameterTaobao parameterTaobao)
    {
        return TOPClientFactory.getTaobaoClient();
    }

    public static void main(String[] args)
    {

        PageQuery pageQuery = new PageQuery(0, 100);
        System.out.println(pageQuery.getStartIndex());

        TradeGetResponse response = new TradeGetResponse();
        System.out.println(response.isSuccess());

        boolean isReceived = false;
        String stepMsg = "浙江省温州市苍南县灵溪分部公司已签收";
        if (stepMsg.contains("已签收"))
        {
            isReceived = Boolean.TRUE;
        } else if (stepMsg.contains("签收人"))
        {
            isReceived = Boolean.TRUE;
        } else if (stepMsg.contains("派送成功"))
        {
            isReceived = Boolean.TRUE;
        } else if (stepMsg.contains("已被"))
        {
            isReceived = Boolean.TRUE;
        } else if (stepMsg.contains("签收"))
        {
            isReceived = Boolean.TRUE;
        }
        System.out.println(" OK : " + isReceived);
        String string = "{'companies':'中通速递 圆通速递 韵达快运','provinces':'北京 天津 河北省 山西省 辽宁省 吉林省 黑龙江省 福建省 江西省 山东省 河南省 湖北省 湖南省 广东省 广西壮族自治区 重庆 四川省 贵州省 湖北省 湖南省 广东省 广西壮族自治区 重庆 四川省 贵州省 云南省 陕西省','time':'6'}";
        Set<String> set = new HashSet<String>();
        JsonObject json = new JsonParser().parse(string).getAsJsonObject();
        StringBuilder sb = new StringBuilder();
        if (json.get("time") != null)
        {
            Integer time = Integer.parseInt((json.get("time").toString()).replace("\"", ""));
            if (json.get("companies") != null)
            {
                String[] companies = (json.get("companies").toString()).split(" ");

                for (String str : companies)
                {
                    if (json.get("provinces") != null)
                    {
                        String[] provinces = (json.get("provinces").toString()).split(" ");
                        for (String p : provinces)
                        {
                            sb.setLength(0);
                            sb.append(str).append(":");
                            sb.append(p).append(":");
                            sb.append(time);
                            set.add((sb.toString()).replace("\"", ""));
                        }
                    }
                }
            }
        }
        Iterator<String> it = set.iterator();
        while (it.hasNext())
        {
            String ccc = it.next();
            if (StringUtils.equalsIgnoreCase(ccc, "圆通速递:河北省:6"))
            {
                System.out.println(" Iiiiiiiiiiiiiiiiiiiiii ");
            }
        }
    }

    /**
     * 计算顺序，已经在推送的排在前面，然后按接入时间排列，最近接的先计算
     * 
     * @param o1
     * @param o2
     * @return
     */
    private int compareSeller(SellerDo o1, SellerDo o2)
    {
        try
        {
            if (o1.getSessionkeyExpires() != null && o2.getSessionkeyExpires() != null)
            {
                if (o1.getSessionkeyExpires().intValue() == 0 && o2.getSessionkeyExpires() == 0)
                {
                    if (o1.getGmtCreate().after(o2.getGmtCreate()))
                    {
                        return -1;
                    } else
                    {
                        return 1;
                    }
                } else if (o1.getSessionkeyExpires() == 0)
                {
                    return -1;
                } else if (o2.getSessionkeyExpires() == 0)
                {
                    return 1;
                } else
                {
                    if (o1.getGmtCreate().after(o2.getGmtCreate()))
                    {
                        return -1;
                    } else
                    {
                        return 1;
                    }
                }
            } else if (o1.getSessionkeyExpires() != null && o1.getSessionkeyExpires() == 0)
            {
                return -1;
            } else if (o2.getSessionkeyExpires() != null && o2.getSessionkeyExpires() == 0)
            {
                return 1;
            } else
            {
                if (o1.getGmtCreate().after(o2.getGmtCreate()))
                {
                    return -1;
                } else
                {
                    return 1;
                }
            }
        } catch (Exception e)
        {
            return 0;
        }
    }
    
    private boolean getTaskToken(Long sellerKey){
		Boolean priValue = runningSellers.putIfAbsent(sellerKey, Boolean.TRUE);
		return (priValue==null);
	}
    
    private void releaseTaskToken(Long sellerId){
    	runningSellers.remove(sellerId);
	}

}
