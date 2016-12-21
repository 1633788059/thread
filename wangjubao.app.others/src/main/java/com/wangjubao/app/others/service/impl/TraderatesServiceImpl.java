package com.wangjubao.app.others.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.taobao.api.DefaultTaobaoClient;
import com.taobao.api.TaobaoClient;
import com.taobao.api.domain.TradeRate;
import com.taobao.api.request.TraderatesGetRequest;
import com.taobao.api.response.TraderatesGetResponse;
import com.wangjubao.app.others.service.TraderatesService;
import com.wangjubao.dolphin.biz.dal.extend.bo.ProductBo;
import com.wangjubao.dolphin.biz.dao.BuyerDao;
import com.wangjubao.dolphin.biz.dao.SellerDao;
import com.wangjubao.dolphin.biz.dao.TradeDao;
import com.wangjubao.dolphin.biz.model.BuyerDo;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.biz.model.SellerProductSubscribeDo;
import com.wangjubao.dolphin.biz.model.SellerTraderateDo;
import com.wangjubao.dolphin.biz.model.SellerTraderateProcessDo;
import com.wangjubao.dolphin.biz.model.TradeDo;
import com.wangjubao.dolphin.biz.model.extend.AppParameterTaobao;
import com.wangjubao.dolphin.biz.service.CompanyService;
import com.wangjubao.dolphin.biz.service.SellerSessionKeyService;
import com.wangjubao.dolphin.biz.service.SellerTraderateService;
import com.wangjubao.dolphin.biz.service.job.AppContext;
import com.wangjubao.dolphin.common.util.StrUtils;
import com.wangjubao.dolphin.common.util.date.DateUtils;
import com.wangjubao.framework.util.StrUtil;

@Service("traderatesService")
public class TraderatesServiceImpl implements TraderatesService {

    public static final Logger                        logger         = Logger.getLogger("traderates");
    // @Autowired
    private SellerDao                                 sellerDao;

    // @Autowired
    private SellerSessionKeyService                   sellerSessionKeyService;

    // @Autowired
    private SellerTraderateService                    sellerTraderateService;

    private CompanyService                            companyService;

    private BuyerDao                                  buyerDao;

    private TradeDao                                  tradeDao;

    private ProductBo                                 productBo;

    private static final Map<String, ExecutorService> EXECUTOR_MAP   = new ConcurrentHashMap<String, ExecutorService>();

    private static final Map<String, Boolean>         EXECUTE_STATUS = new ConcurrentHashMap<String, Boolean>();

    public static Map<String, String>                 TEST_MAP       = null;

    static
    {
        TEST_MAP = new ConcurrentHashMap<String, String>();
        TEST_MAP.put("南慕小记", ">>>>>>");
        TEST_MAP.put("南慕数码专营店", ">>>>>>");
        TEST_MAP.put("驭派数码专营", ">>>>>>");
        TEST_MAP.put("rock洛克慕家专卖店", ">>>>>>");
        TEST_MAP.put("电小二慕家专卖店", ">>>>>>");
        TEST_MAP.put("粤酷数码专营店", ">>>>>>");
        TEST_MAP.put("mooke旗舰店", ">>>>>>");
        TEST_MAP.put("黑鱼旗舰店", ">>>>>>");
        TEST_MAP.put("三星彩声专卖店", ">>>>>>");
        TEST_MAP.put("驿道数码专营店", ">>>>>>");
        TEST_MAP.put("捷点数码专营店", ">>>>>>");
        TEST_MAP.put("mooke间优专卖店", ">>>>>>");
        TEST_MAP.put("展天数码专营店", ">>>>>>");
        TEST_MAP.put("rock洛克彩声专卖店", ">>>>>>");
    }

    public TraderatesServiceImpl()
    {
        super();
        // ApplicationContext ctx = new ClassPathXmlApplicationContext(
        // "classpath*:/META-INF/spring/*.xml");
        sellerDao = (SellerDao) AppContext.getBean("sellerDao");
        sellerSessionKeyService = (SellerSessionKeyService) AppContext.getBean("sellerSessionKeyService");
        sellerTraderateService = (SellerTraderateService) AppContext.getBean("sellerTraderateService");
        productBo = (ProductBo) AppContext.getBean("productBo");
        companyService = (CompanyService) AppContext.getBean("companyService");
        buyerDao = (BuyerDao) AppContext.getBean("dolphinBuyerDao");
        tradeDao = (TradeDao) AppContext.getBean("tradeDao");
        logger.info("拉取评价线程初始化完成..." + sellerSessionKeyService.toString());
    }

    public static void main(String[] args) throws Exception
    {
        System.out.println(DateUtils.formatDate(new Date(1404338399000l)));
        //        TraderatesServiceImpl traderatesService = new TraderatesServiceImpl();
        //        //logger.info(traderatesService.toString());
        //        traderatesService.execute();
        //        logger.info(new Date().getTime());
    }

    @Override
    public void execute()
    {
        logger.info(" ####################################### 拉取评价启动..." + DateUtils.formatDate(DateUtils.now()));
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(2);
        scheduledThreadPoolExecutor.scheduleWithFixedDelay(new Runnable()
        {

            @Override
            public void run()
            {
                try
                {
                    if (DateUtils.nowHour() < 22 && DateUtils.nowHour() > 5)
                    {
                        getTradeRates();
                    }
                } catch (Exception e)
                {
                    e.printStackTrace();
                    logger.error(StrUtils.showError(e));
                }
            }

        }, 6, 60 * 30, TimeUnit.SECONDS);
    }

    /**
     * 传说中的并发读取
     */
    private void getTradeRates() throws Exception
    {
        List<SellerDo> aSellerlist = sellerDao.listAll();// 32
        //        List<CompanySellerDo> cSellerList = sellerDao.listCompanySeller(); // 获取签约客户的店铺
        //        logger.info("seller数目：：" + cSellerList.size());
        //        Map<String, Object> cSellerMap = new HashMap<String, Object>();
        //        for (CompanySellerDo csd : cSellerList) {
        //            cSellerMap.put(csd.getSellerId().toString(), csd);
        //        }

        List<SellerDo> sellerList = new ArrayList<SellerDo>(); // 只拉取签约客户的店铺评价
        for (SellerDo sd : aSellerlist)
        {
            if (sd.getId().longValue() != 741598534l)
            {
                //                continue;
            }
            if (sd.getSellerType() == 1 || sd.getSellerType() == 8)
            {
                //                if (cSellerMap.get(sd.getId().toString()) != null) {
                //                    sellerList.add(sd);
                //                }
                if (companyService.isValidCompany(sd.getId()))
                {
                    sellerList.add(sd);
                }
            }
        }
        logger.info("拉取店铺数(正式客户数)>>>>>>>>>>：" + sellerList.size());
        for (final SellerDo sellerDo : sellerList)
        {

            if (!TEST_MAP.containsKey(sellerDo.getNick()))
            {
                //continue;
            }

            ExecutorService executor = EXECUTOR_MAP.get(sellerDo.getDatasourceKey());
            if (executor == null)
            {
                executor = Executors.newSingleThreadExecutor();
                EXECUTOR_MAP.put(sellerDo.getDatasourceKey(), executor);
            }
            executor.execute(new Runnable()
            {

                @Override
                public void run()
                {
                    try
                    {
                        if (addStatus(sellerDo))
                        {
                            executeForSeller(sellerDo);
                        }
                    } catch (Exception e)
                    {
                        e.printStackTrace();
                        logger.error(StrUtils.showError(e));
                    } finally
                    {
                        delStatus(sellerDo);
                    }
                }
            });
        }
    }

    private synchronized void delStatus(SellerDo sellerDo)
    {
        EXECUTE_STATUS.remove(sellerDo.getId().toString());
    }

    private synchronized boolean addStatus(SellerDo sellerDo)
    {
        if (EXECUTE_STATUS.containsKey(sellerDo.getId().toString()))
        {
            return false;
        } else
        {
            EXECUTE_STATUS.put(sellerDo.getId().toString(), Boolean.TRUE);
        }
        return true;
    }

    private void executeForSeller(SellerDo sellerDo) throws Exception
    {

        List<SellerProductSubscribeDo> sublist = productBo.getSubscribeProductBySellerId(sellerDo.getId());
        AppParameterTaobao prameter = null;
        for (SellerProductSubscribeDo sellerProductSubscribeDo : sublist)
        {
            prameter = sellerSessionKeyService.parameter2Bean(sellerProductSubscribeDo);
            if (prameter != null)
            {
                if (StringUtils.equalsIgnoreCase(prameter.getAppkey(), "12251093"))
                {
                    break;
                } else
                {
                    prameter = null;
                    continue;
                }
            }
        }

        if (prameter == null)
        {
            logger.info(" prameter是null ..... nick::" + sellerDo.getNick());
            return;
        }

        String appkey = prameter.getAppkey();
        String secret = prameter.getSecret();
        String sessionKey = prameter.getAccessToken();

        if (StringUtils.isEmpty(appkey) || StringUtils.isEmpty(secret) || StringUtils.isEmpty(sessionKey))
        {
            logger.info(" sessionKey 可能为null ..... nick::" + sellerDo.getNick());
            return;
        }

        if (StrUtil.isNotEmpty(prameter.getExpiresIn()))
        {
            if (System.currentTimeMillis() > Long.valueOf(prameter.getExpiresIn()).longValue())
            {
                logger.error(String.format("[%s-授权过期... %s]", sellerDo.getNick(),
                        DateUtils.formatDate(new Date(Long.valueOf(prameter.getExpiresIn())))));
                return;
            }
        } else
        {
            logger.error(String.format("[%s-授权过期... ]", sellerDo.getNick()));
            return;
        }

        //        String sessionKey = sellerSessionKeyService.getSessionKeyById(sellerDo.getId(),
        //                ProductType.CRM.getValue());        
        // 从t_seller_traderate_process表中获取已更新进度记录，start_time字段作为读接口的起始时间，不存在的话就插入
        SellerTraderateProcessDo stpDo = sellerTraderateService.queryProcessById(sellerDo.getId());
        Date d1 = null;
        Date d2 = null;
        if (stpDo == null || stpDo.getStartTime() == null)
        {
            d1 = DateUtils.nextDays(DateUtils.now(), -180);
        } else
        {
            d1 = DateUtils.getNextSecondForDate(stpDo.getStartTime(), (1 - (60 * 5)));//反推5min
        }

        if (d1.after(DateUtils.getNextSecondForDate(DateUtils.now(), 60 * 60 * 4))) // 2个小时之内的评价下次再处理
        {
            logger.info(String.format("[%s----%s 下次再处理.]", sellerDo.getNick(), DateUtils.formatDate(d1)));
            return;
        }

        d2 = DateUtils.nextDays(d1, 1);
        d2 = getD2(d1, d2, appkey, secret, sessionKey);
        if (d2 == null)
        {
            logger.error(String.format("[%s-调用接口出错...无法获取结束时间.]", sellerDo.getNick()));
            return;
        }

        long mseconds1 = d2.getTime() - d1.getTime();
        mseconds1 = mseconds1 / 2;
        if (mseconds1 == 0l)
        {
            mseconds1 = 10000000l;
        }
        //        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //        int errors = 0;
        do
        {
            TraderatesGetRequest req = new TraderatesGetRequest();
            req.setFields("tid,oid,role,rated_nick,nick,result,created,item_title,item_price,content,reply,num_iid");
            req.setRateType("get");
            req.setRole("buyer");
            req.setPageSize(100L);
            req.setStartDate(d1);
            req.setEndDate(d2);

            TraderatesGetResponse response = getResponse(req, appkey, secret, sessionKey, null);
            if (response != null && response.isSuccess())
            {
                //                int totalResults = Integer.valueOf(res.getTotalResults() + "");
                long totalResults = response.getTotalResults();
                logger.info(String.format("[{%s}::totalResults: %s] 获取区间:(%s ~ %s)", sellerDo.getNick(), totalResults,
                        DateUtils.formatDate(d1), DateUtils.formatDate(d2)));
                if (totalResults == 0)
                {
                    d1 = d2;
                    d2 = new Date(d2.getTime() + mseconds1);
                    if (stpDo == null)
                    {
                        stpDo = new SellerTraderateProcessDo();
                        stpDo.setSellerId(sellerDo.getId());
                        stpDo.setNick(sellerDo.getNick());
                        stpDo.setStartTime(d2);
                        stpDo.setGmtCreated(DateUtils.now());
                        stpDo.setGmtModified(DateUtils.now());
                        sellerTraderateService.insertProcess(stpDo);
                    } else
                    {
                        if (d2.after(DateUtils.now()))
                        {
                            d2 = DateUtils.now();
                        }
                        stpDo.setStartTime(d2);
                        stpDo.setGmtModified(new Date());
                        sellerTraderateService.updateProcess(stpDo);
                    }
                } else if (totalResults > 0 && totalResults < 1500)
                {
                    d1 = d2;
                    d2 = new Date(d2.getTime() + mseconds1);
                    long pageNo = totalResults % 100 == 0 ? (totalResults / 100) : ((totalResults / 100) + 1);
                    //                    int errorTimes = 0;
                    for (long i = 1; i <= pageNo; i++)
                    {
                        req.setPageNo(i);
                        response = getResponse(req, appkey, secret, sessionKey, null);
                        if (response != null && response.isSuccess())
                        {
                            List<TradeRate> rateList = response.getTradeRates();
                            saveTradeRate(sellerDo.getId(), rateList);//	
                            if (stpDo == null)
                            {
                                stpDo = new SellerTraderateProcessDo();
                                stpDo.setSellerId(sellerDo.getId());
                                stpDo.setNick(sellerDo.getNick());
                                stpDo.setStartTime(d2);
                                stpDo.setGmtCreated(new Date());
                                stpDo.setGmtModified(new Date());
                                sellerTraderateService.insertProcess(stpDo);
                            } else
                            {
                                if (d2.getTime() > new Date().getTime())
                                {
                                    d2 = new Date();
                                }
                                stpDo.setStartTime(d2);
                                stpDo.setGmtModified(new Date());
                                sellerTraderateService.updateProcess(stpDo);
                            }
                        } else
                        {
                            logger.error(String.format("[%s ---调用接口出错...]", sellerDo.getNick()));
                            //                            errorTimes++;
                            //                            i--;
                            //                            if (errorTimes > 10) { //连续10次调接口失败就不搞了
                            //                                break;
                            //                            }
                        }
                    }
                } else if (totalResults >= 1500)
                {
                    mseconds1 = mseconds1 / 2;
                    d2 = new Date(d1.getTime() + mseconds1);
                    logger.info(String.format("[{%s-评价数据大于淘宝限制数条数1500条count:(%s),重新切分时间点. %s-%s}]", sellerDo.getNick(), totalResults,
                            DateUtils.formatDate(d1), DateUtils.formatDate(d1)));
                } else if (totalResults < 100 && mseconds1 < 10000000l)
                {
                    mseconds1 = 10000000l;
                } else
                {
                    logger.error("一切都不在计划之内...请立即终止应用程序,检查原因.");
                }
            } else
            {
                logger.error(String.format("[%s ---调用接口出错...]", sellerDo.getNick()));
                //                d1 = DateUtils.nextDays(d1, -1);
                //                d2 = DateUtils.nextDays(d2, -1);
                //                errors++;
                //                if (errors > 20) { //一家店铺累计失败20次就不搞了
                //                    d1 = new Date();
                //                }
            }
        } while (DateUtils.diffSecond(DateUtils.now(), d1) > 200);
        logger.info(" <<<<<<<<<<<<<<< GAME OVER " + sellerDo.getNick());
    }

    /**
     * 接口每次最多只能返回1500条，我们每次最多只拿1499条，否则重新获取date2时间
     * 
     * @param date1
     * @param date2
     * @param sessionKey
     * @return
     * @throws Exception
     */
    private Date getD2(Date date1, Date date2, String appkey, String secret, String sessionKey) throws Exception
    {
        TraderatesGetRequest req = new TraderatesGetRequest();
        req.setFields("tid,oid,role,rated_nick,nick,result,created,item_title,item_price,content,reply");
        req.setRateType("get");
        req.setRole("buyer");
        req.setStartDate(date1);
        req.setEndDate(date2);
        TraderatesGetResponse res = getResponse(req, appkey, secret, sessionKey, null);

        if (res != null && res.isSuccess())
        {
            if (res.getTotalResults() < 1500)
            {
                return date2;
            } else
            {
                long mseconds = date2.getTime() - date1.getTime();
                mseconds = mseconds / 2;
                date2 = new Date(date2.getTime() - mseconds);
                return getD2(date1, date2, appkey, secret, sessionKey);
            }
        } else
        {
            if (res != null)
            {
                logger.error(String.format("调接口失败 ErrorCode:[%s] Msg:[%s]", res.getErrorCode(), res.getMsg()));
            } else
            {
                logger.error("-获取 结束时间--------------调用接口返回null");
            }
            return null;
        }
    }

    /**
     * 保存TradeRate至库
     * 
     * @param sellerId
     * @param list
     */
    private void saveTradeRate(Long sellerId, List<TradeRate> list)
    {
        Gson gson = new GsonBuilder().create();
        java.lang.reflect.Type type = new com.google.gson.reflect.TypeToken<List<TradeRate>>()
        {
        }.getType();

        for (int i = 0; i < list.size(); i++)
        {
            TradeRate tradeRate = list.get(i);
            List<TradeRate> rateList = new ArrayList<TradeRate>();
            rateList.add(tradeRate);
            SellerTraderateDo sellerTraderate = new SellerTraderateDo();
            sellerTraderate.setSellerId(sellerId);
            sellerTraderate.setOid(tradeRate.getOid().toString());
            sellerTraderate = sellerTraderateService.loadByOid(sellerTraderate);

            if (sellerTraderate == null)
            {
                sellerTraderate = new SellerTraderateDo();
                sellerTraderate.setSellerId(sellerId);
                sellerTraderate.setTid(tradeRate.getTid() + "");
                TradeDo trade = tradeDao.findBySourceId(sellerTraderate.getSellerId(), sellerTraderate.getTid());

                sellerTraderate.setRateNick(tradeRate.getNick());
                if (trade != null)
                    sellerTraderate.setRateNick(trade.getBuyerNick());
                sellerTraderate.setRateKeyword(tradeRate.getContent());
                sellerTraderate.setCreated(tradeRate.getCreated());
                sellerTraderate.setRateResult(tradeRate.getResult());
                if (StrUtil.isNotEmpty(tradeRate.getContent()))
                {
                    sellerTraderate.setTradeRateSize(tradeRate.getContent().length() + "");
                } else
                {
                    sellerTraderate.setTradeRateSize("0");
                }
                sellerTraderate.setOid(tradeRate.getOid() + "");
                sellerTraderate.setNumIid(tradeRate.getNumIid() + "");
                sellerTraderate.setRatejson(gson.toJson(rateList, type));
                sellerTraderate.setGmtCreated(new Date());
                sellerTraderate.setGmtModified(new Date());
                sellerTraderate.setStatus(0);
                sellerTraderateService.create(sellerTraderate);

                if (trade != null)
                {
                    tradeDao.updateGmtModified(trade);
                }
                BuyerDo buyer = buyerDao.findByNick(sellerTraderate.getSellerId(), sellerTraderate.getRateNick());
                if (buyer != null)
                {
                    //                    buyerDao.update(buyer);
                    buyerDao.updateGmtModified(buyer);
                }
                //                if (i % 10 == 0) {
                //                    logger.info("insert::oid==" + sellerTraderate.getOid() + " nick : " + tradeRate.getRatedNick());
                //                }
            } else
            {
                //                try
                //                {
                List<TradeRate> list1 = gson.fromJson(sellerTraderate.getRatejson(), type);
                if (list1.size() > 1)
                {
                    rateList.add(list1.get(1));
                }
                TradeDo trade = tradeDao.findBySourceId(sellerTraderate.getSellerId(), sellerTraderate.getTid());
                sellerTraderate.setRatejson(gson.toJson(rateList, type));
                if (trade != null)
                    sellerTraderate.setRateNick(trade.getBuyerNick());
                Boolean flag = sellerTraderateService.update(sellerTraderate);
                if (trade != null)
                {
                    //                        tradeDao.update(trade);
                    tradeDao.updateGmtModified(trade);
                }
                BuyerDo buyer = buyerDao.findByNick(sellerTraderate.getSellerId(), sellerTraderate.getRateNick());
                if (buyer != null)
                {
                    buyerDao.updateGmtModified(buyer);
                }
                //	                if (i % 10 == 0) {
                //	                    logger.info("update::flag==" + flag + " nick : " + tradeRate.getRatedNick());
                //	                }

                //                } catch (Exception e)
                //                {
                //                    e.printStackTrace();
                //                    continue;
                //                }
            }
        }
    }

    /**
     * 获取评价信息
     * 
     * @param req
     * @return
     */
    private TraderatesGetResponse getResponse(TraderatesGetRequest request, String appkey, String secret, String sessionKey,
                                              Integer flag)
    {
        int errorcount = 1;
        if (flag == null)
        {
            flag = 0;
        }
        TraderatesGetResponse response = null;
        TaobaoClient client = new DefaultTaobaoClient("http://gw.api.taobao.com/router/rest", appkey, secret);
        try
        {
            if (flag > 0)
            {
                TimeUnit.MILLISECONDS.sleep(500 * 2 * flag);
            }
            response = client.execute(request, sessionKey);
        } catch (Exception e)
        {
            e.printStackTrace();
            logger.error(StrUtils.showError(e));
        }
        if (response == null || !response.isSuccess())
        {

            if (response != null)
            {
                logger.error("\t\r\n 调用淘宝接口失败  ErrorCode:" + response.getErrorCode() + " msg: " + response.getMsg());
            } else
            {
                logger.error(" ################## 接口返回 NULL...");
            }

            if (StringUtils.equalsIgnoreCase("27", response.getErrorCode()))
            {
                return response;
            }
            if (flag < 3)
            {
                if (StringUtils.equalsIgnoreCase("7", response.getErrorCode()))
                {
                    try
                    {
                        flag = 1;
                        if (++errorcount > 50)
                        {
                            logger.error(" 怎么会这样？------------------");
                            return response;
                        }
                        TimeUnit.MILLISECONDS.sleep(1000 * 10 * errorcount);
                        logger.error("调用失败,重试中..." + errorcount + " startTime : " + DateUtils.formatDate(request.getStartDate())
                                + " endTime : " + DateUtils.formatDate(request.getEndDate()));
                    } catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }
                getResponse(request, appkey, secret, sessionKey, ++flag);
            }
        }
        return response;
    }

}
