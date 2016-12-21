package com.wangjubao.app.others.sellerInfo;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.wangjubao.app.others.dayreport.SellerDayReport;
import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.common.constant.SellerType;
import com.wangjubao.dolphin.biz.dao.SellerDao;
import com.wangjubao.dolphin.biz.dao.SellerProductSubscribeDao;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.biz.model.SellerProductSubscribeDo;
import com.wangjubao.dolphin.biz.model.extend.AppParameterTaobao;
import com.wangjubao.dolphin.common.lang.Paginator;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.date.DateUtils;
import com.wangjubao.framework.util.DateUtil;

public class SellerDataServer {
//    static final Logger                         logger                   = Logger.getLogger(SellerDataServer.class);
    private transient static Logger              dayreprot                = Logger.getLogger("dayreport");
    private SellerDayReport                     sellerDayReport          = null;
    String                                      appKey                   = "12251093";
    private SellerDao                           sellerDao;
    private Gson                                gson                     = new Gson();

    private SellerProductSubscribeDao           sellerProductSubscribeDao;
    private static boolean                       hasInitOver              = false;
    // 卖家信息
    private static Map<Long, SellerDo>           sellerMap                = new ConcurrentHashMap<Long, SellerDo>();
    // 卖家对应的SessionKey
    private static Map<Long, AppParameterTaobao> SellerSessionKey         = new ConcurrentHashMap<Long, AppParameterTaobao>();
    // 过期的店铺SessionKey
    private static Map<Long, AppParameterTaobao> ExpiresSellerSessionKey  = new ConcurrentHashMap<Long, AppParameterTaobao>();
    // 已经添加到DataPush的 店铺session
    private static Map<Long, AppParameterTaobao> DataPushSellerSessionKey = new ConcurrentHashMap<Long, AppParameterTaobao>();

    // 数据库对应实例的关系
    private static Map<String, Set<Long>>        SellerDataSource         = new ConcurrentHashMap<String, Set<Long>>();
    // 卖家和数据源的对应关系
    private static Map<Long, String>             SellerToDataSource       = new ConcurrentHashMap<Long, String>();
    // 当前正在计算的卖家任务 在数据源上的 对应
    private static Map<String, Set<Long>>        SellerTaskDataSource     = new ConcurrentHashMap<String, Set<Long>>();
    
    private ScheduledExecutorService dailyReportExecutor = Executors.newScheduledThreadPool(1);

    public SellerDataServer() {
        sellerDayReport = new SellerDayReport();
        sellerDao = (SellerDao) SynContext.getObject("sellerDao");
        sellerProductSubscribeDao = (SellerProductSubscribeDao) SynContext
                .getObject("sellerProductSubscribeDao");
    }

    // 判断SessionKey 是否过期
    private boolean isExpiresSessionKey(AppParameterTaobao sellerTaobaoParameter) {
        if (1 < 20) {
            return false;
        }
        Date now = new Date();
        Long nowtime = now.getTime();
        String expiresIn = sellerTaobaoParameter.getExpiresIn();
        Long expirestime = 0L;
        try {
            expirestime = Long.parseLong(expiresIn);

        } catch (NumberFormatException e) {
        }
        if (nowtime > expirestime) {
            return true;
        }
        return false;

    }

    @SuppressWarnings("unused")
    private void putMap(SellerDo seller, SellerProductSubscribeDo subscribeDo) {
        if (seller != null
                && (seller.getSellerType() == SellerType.TAOBAO || seller.getSellerType() == SellerType.TMALL)) {
            Long key = seller.getId();
            sellerMap.put(key, seller);
            AppParameterTaobao sellerTaobaoParameter = null;

            try {
                sellerTaobaoParameter = gson.fromJson(subscribeDo.getParameter(),
                        AppParameterTaobao.class);

            } catch (JsonSyntaxException e) {
            }

            if (sellerTaobaoParameter != null) {
                boolean isEpires = isExpiresSessionKey(sellerTaobaoParameter);
                if (isEpires) {
                    ExpiresSellerSessionKey.put(key, sellerTaobaoParameter);
                } else {
                    SellerSessionKey.put(key, sellerTaobaoParameter);
                }
            }
            if (seller.getDatasourceKey() != null && !hasInitOver) {
                Set<Long> sellerList = SellerDataSource.get(seller.getDatasourceKey());

                if (sellerList == null) {
                    sellerList = new HashSet<Long>();
                }

                sellerList.add(seller.getId());
                SellerDataSource.put(seller.getDatasourceKey(), sellerList);
                SellerToDataSource.put(seller.getId(), seller.getDatasourceKey());
            }

        }
    }

    public void doDataFetch() {
        dayreprot.info(" ################################ start...");
        
        dailyReportExecutor.scheduleAtFixedRate(new Runnable(){
			@Override
			public void run() {
				try{
					int hour = DateUtils.nowHour();
					if(hour<=6 || hour>=10)
						return;
					List<SellerDo> sellerList = sellerDao.listActiveCompanySeller();
					for(SellerDo seller : sellerList){
						if (seller != null && seller.getAcceptedDayreport()
	                            && (seller.getSellerType() == SellerType.TAOBAO || seller.getSellerType() == SellerType.TMALL 
	                                || seller.getSellerType().intValue() == SellerType.JINGDONG)) {
							if(seller.getSellerType().intValue() == SellerType.JINGDONG){
								if(seller.getLastSynTime()==null || seller.getLastSynTime().before(DateUtils.startOfDate(DateUtils.now())))
									continue;
							}
							Date lastNotifyTime = seller.getLastNotifyTime();
							if(DateUtils.isSameDay(lastNotifyTime, DateUtils.now()))
							    continue;	
							//发送日报
		                    if (sellerDayReport.SendDayReport(seller)) {
		                        seller.setLastNotifyTime(new Date());
		                        sellerDao.updatelastNotifyTime(seller);
		                    } else {
	                            dayreprot.info(String.format("{%s【%s】} 日报发送失败，下次再重试。", seller.getId(),
	                                    seller.getNick()));
		                    }
						}
					}
				}catch(Throwable t){
					dayreprot.error("Fail to send day report", t);
				}
			}
        	
        }, 1, 1, TimeUnit.HOURS);

        /*
        new Thread() {
            public void run() {
                while (true) {

                    String tstart = "60000";
                    Integer endt = Integer.parseInt(tstart) + 40000;
                    String timenow = DateUtil.getTimeStr().replace(":", "");
                    boolean timeIsOk = false;
                    if (Integer.parseInt(timenow) >= Integer.parseInt(tstart)
                            && Integer.parseInt(timenow) <= endt) {
                        timeIsOk = true;
                    }
                    if (!timeIsOk) {
                        try {
                            sleep(1000 * 60 * 30);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        continue;
                    }

                    // 查询所有卖家
                    SellerDataSource.clear();
                    SellerSessionKey.clear();
                    sellerMap.clear();
                    PageQuery pageQuery = new PageQuery(0, 4000);
                    while (true) {

                        PageList<SellerDo> resultDo = sellerDao
                                .listAll(null, null, null, pageQuery);

                        if (resultDo == null) {
                            break;
                        }

                        for (SellerDo seller : resultDo) {
                            if (seller != null && seller.getAcceptedDayreport()// && (seller.getId().longValue()==903132895l ||seller.getId().longValue()==1084277529l ||seller.getId().longValue()==792945115l || seller.getId().longValue()==120055122l)
                                    && (seller.getSellerType() == SellerType.TAOBAO || seller
                                            .getSellerType() == SellerType.TMALL || seller.getSellerType().intValue() == SellerType.JINGDONG)) {
                                //                                logger.info("-------" + seller.getNick());
                                List<SellerProductSubscribeDo> list = null;
                                if (seller.getId() != null) {
                                    //                                    try {
                                    //                                        sleep(200);
                                    list = sellerProductSubscribeDao.listBySellerId(seller.getId());
                                    //                                    } catch (InterruptedException e) {
                                    //                                        e.printStackTrace();
                                    //                                    }
                                }

                                if (list != null) {
                                    for (SellerProductSubscribeDo model : list) {
                                        // 获取产品订购信息 
                                        if (model.getProductId().intValue() == 1) {
                                            dayreprot.info(String.format(" -- {%s【%s】}",
                                                    seller.getId(), seller.getNick()));
                                            putMap(seller, model);
                                        }
                                    }
                                }

                            }
                        }

                        Paginator paginator = resultDo.getPaginator();
                        if (paginator.getNextPage() == paginator.getPage()) {
                            break;
                        }
                        pageQuery.increasePageNum();

                    }

                    //                    dayreprot.info("--------淘宝合法卖家数量：" + SellerSessionKey.size());

                    doOrderJob();

                    try {
                        sleep(1000 * 60 * 30);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }
            }
        }.start();
        */
    }

    private void doOrderJob() {
        AppParameterTaobao sessionInf;
        Long sellerId;
//        logger.info("--------------日报---");
        for (Object o : SellerSessionKey.keySet()) {
            sellerId = Long.parseLong(o.toString());
            sessionInf = SellerSessionKey.get(sellerId);

            if (sessionInf.getAppkey().equals(appKey)) {
                try {

                    SellerDo seller = sellerMap.get(sellerId);
                    seller = sellerDao.load(seller.getId());
                    //对比lastNotifyTime和今天，如果相等代表已经发过不需再发。
                    Date lastNotifyTime = seller.getLastNotifyTime();

                    boolean hasDone = false;
                    if (lastNotifyTime != null) {
                        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
                        String dayUpdate = sf.format(lastNotifyTime);
                        String dayNow = sf.format(new Date());
                        hasDone = dayNow.equals(dayUpdate);
                    }

                    // 测爱宁
                    //                    if (seller.getId().longValue() != 898637267l) {
                    if (hasDone) {
                        //                        dayreprot.info(String.format("{%s【%s】} 日报于 ｛%s｝已经发送。", seller.getId(),
                        //                                seller.getNick(), DateUtils.formatDate(lastNotifyTime)));
                        continue;
                    }
                    //                    }

                    //发送日报
                    if (sellerDayReport.SendDayReport(seller)) {
                        //更新发送日报时间
                        seller.setLastNotifyTime(new Date());
                        sellerDao.updatelastNotifyTime(seller);
                        //                        dayreprot.info(String.format("{%s【%s】} 日报发送完成 更新本次发送时间【%s】", seller.getId(),
                        //                                seller.getNick(), DateUtils.formatDate(seller.getLastNotifyTime())));
                    } else {
                        //通过getAcceptedDayreport获取卖家是否设置接受。通过getNotifyEmails获取发送邮箱
                        if (seller.getAcceptedDayreport()) {
                            dayreprot.info(String.format("{%s【%s】} 日报发送失败，下次再重试。", seller.getId(),
                                    seller.getNick()));
                        }
                    }

                } catch (Exception e) {
                    dayreprot.error(" 发送失败 ： " + Arrays.toString(e.getStackTrace()));
                }
            }
        }
    }
}
