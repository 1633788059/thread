package com.wangjubao.app.others.service.impl;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.drools.lang.dsl.DSLMapParser.condition_key_return;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.taobao.api.DefaultTaobaoClient;
import com.taobao.api.TaobaoClient;
import com.wangjubao.app.others.service.BuyerInfoService;
import com.wangjubao.dolphin.biz.common.constant.ProductType;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dal.extend.bo.ProductBo;
import com.wangjubao.dolphin.biz.common.constant.SellerType;
import com.wangjubao.dolphin.biz.dao.BuyerDao;
import com.wangjubao.dolphin.biz.dao.SellerDao;
import com.wangjubao.dolphin.biz.dao.SellerProductSubscribeDao;
import com.wangjubao.dolphin.biz.model.BuyerDo;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.biz.model.SellerProductSubscribeDo;
import com.wangjubao.dolphin.biz.model.extend.AppParameterTaobao;
import com.wangjubao.dolphin.biz.service.CompanyService;
import com.wangjubao.dolphin.biz.service.SellerService;
import com.wangjubao.dolphin.common.lang.Paginator;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.StrUtils;
import com.wangjubao.dolphin.common.util.date.DateUtils;

/**
 * @author ckex created 2013-6-19 - 下午1:30:42 BuyerInfoServiceImpl.java
 * @explain - buyer信息更新
 */
@Service("buyerInfoService")
public class BuyerInfoServiceImpl implements BuyerInfoService {

    private transient final static Logger            logger       = LoggerFactory.getLogger("buyer-info");

    @Autowired
    private SellerService                            sellerService;

    @Autowired
    private SellerDao                                sellerDao;

    @Autowired
    @Qualifier("dolphinBuyerDao")
    private BuyerDao                                 buyerDao;

    @Autowired
    private SellerProductSubscribeDao                sellerProductSubscribeDao;

    @Autowired
    private ProductBo                                productBo;

    @Autowired
    private CompanyService                           companyService;

    private static final int                         maxThread    = 2;

    private static Map<String, ThreadPoolExecutor>   EXECUTOR_MAP = new ConcurrentHashMap<String, ThreadPoolExecutor>();

    //    private static ThreadPoolExecutor              defaultPool = new ThreadPoolExecutor(
    //                                                                       maxThread,
    //                                                                       maxThread,
    //                                                                       100,
    //                                                                       TimeUnit.MILLISECONDS,
    //                                                                       new LinkedBlockingQueue<Runnable>(),
    //                                                                       new ThreadPoolExecutor.AbortPolicy()); // 表示拒绝任务并抛出异常

    private static final Map<Long, AtomicBoolean>    FLAG_MAP     = new ConcurrentHashMap<Long, AtomicBoolean>();

    private static final ScheduledThreadPoolExecutor SCHEDULE     = new ScheduledThreadPoolExecutor(2);

    private static long                              INTERVAL     = 1000 * 60 * 60 * 24;

    @PostConstruct
    @Override
    public void init()
    {
    	/*
        List<SellerDo> seller = sellerDao.listAll();
        int k = 0;
        for (SellerDo sellerDo : seller)
        {
            if (StringUtils.isNotBlank(sellerDo.getDatasourceKey()))
                if (sellerDo.getSellerType().intValue() == SellerType.TAOBAO
                        || sellerDo.getSellerType().intValue() == SellerType.TMALL)
                {
                    if (StringUtils.equalsIgnoreCase(sellerDo.getDatasourceKey(), "002")
                            || StringUtils.equalsIgnoreCase(sellerDo.getDatasourceKey(), "028"))
                    {
                        EXECUTOR_MAP.put(sellerDo.getDatasourceKey(), initThreadPool(maxThread * 2));
                    } else
                    {
                        EXECUTOR_MAP.put(sellerDo.getDatasourceKey(), initThreadPool(maxThread));
                    }
                    ++k;
                }
        }
        if (logger.isInfoEnabled())
        {
            logger.info(" BuyerInfoService initialize finished . seller size  " + k);
        }
        */
    }

    @Override
    public void syncByuerInfo()
    {

        logger.info(" ####################################### 拉会员信息启动..." + DateUtils.formatDate(DateUtils.now()));
        //        new Thread()
        //        {
        //
        //            @Override
        //            public void run()
        //            {
        //                Calendar c = DateUtils.getCalendar();
        //                c.set(Calendar.HOUR_OF_DAY, 22);
        //                c.set(Calendar.MINUTE, 25);
        //                c.set(Calendar.SECOND, 0);
        //                Date startTime = c.getTime();
        //                Timer timer = new Timer();
        //                TimerTask task = new TimerTask()
        //                {
        //
        //                    // buyer信息
        //                    /****************************************************************************/
        //                    @Override
        //                    public void run()
        //                    {
        //
        //                        SCHEDULE.schedule(new Runnable()
        //                        {
        //
        //                            @Override
        //                            public void run()
        //                            {
        //                                try
        //                                {
        //                                    execute();
        //                                } catch (Exception e)
        //                                {
        //                                    e.printStackTrace();
        //                                }
        //
        //                            }
        //                        }, 5, TimeUnit.MINUTES);
        //                    }
        //                };
        //                timer.scheduleAtFixedRate(task, startTime, INTERVAL / 2);
        //            }
        //        }.start();

        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(2);
        scheduledThreadPoolExecutor.scheduleWithFixedDelay(new Runnable()
        {

            @Override
            public void run()
            {
                execute();
            }
        }, 0, 24, TimeUnit.HOURS);
    }

    @Override
    public void execute()
    {
    	executeBuyerInfo(1986899349L);
    	return;
/*
        List<SellerDo> seller = sellerDao.listAll();
        for (SellerDo sellerDo : seller)
        {
            long start = System.currentTimeMillis();
            if (sellerDo.getSellerType() == null
                    || (sellerDo.getSellerType().intValue() != SellerType.TAOBAO && sellerDo.getSellerType().intValue() != SellerType.TMALL))
            {
                continue;
            }
            if (!companyService.isValidCompany(sellerDo.getId()))
            {
                continue;//非签约客户
            }
 */
            /*****************************************************************/
            //            int i = (int) (sellerDo.getId() % 7);
            //            int week = DateUtils.dayOfWeek();
            //            if (i != week)
            //            {
            //                if (logger.isInfoEnabled())
            //                {
            //                    logger.info(String.format(" %s skip, now week 星期( %s ), execute week 星期 ( %s )", sellerDo.getNick(), week, i));
            //                }
            //                continue;
            //            }
            /*****************************************************************/
/*
            if (!filterBySellerId(sellerDo))
            {
                continue;
            }
            if (logger.isInfoEnabled())
            {
                logger.info(sellerDo.getId() + " seller nick : " + sellerDo.getNick());
            }
            try
            {
                if (!FLAG_MAP.containsKey(sellerDo.getId()))
                {
                    FLAG_MAP.put(sellerDo.getId(), new AtomicBoolean(true));
                }
                if (FLAG_MAP.get(sellerDo.getId()).get())
                {
                    FLAG_MAP.get(sellerDo.getId()).set(Boolean.FALSE);
                    if (logger.isInfoEnabled())
                    {
                        logger.info(String.format(" ( %s ) execute synchro buyer info .", sellerDo.getNick()));
                    }
                    if (StringUtils.isNotBlank(sellerDo.getDatasourceKey()))
                    {
                        if (!checkSessionKey(sellerDo))
                        {
                            FLAG_MAP.get(sellerDo.getId()).set(Boolean.TRUE);
                            continue;
                        }
                        ThreadPoolExecutor thread = EXECUTOR_MAP.get(sellerDo.getDatasourceKey());
                        if (thread == null)
                        {
                            thread = initThreadPool(maxThread);
                            EXECUTOR_MAP.put(sellerDo.getDatasourceKey(), thread);
                        }
                        final Long sellerId = sellerDo.getId();
                        thread.execute(new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                executeBuyerInfo(sellerId);
                            }
                        });
                    }
                }
            } catch (Exception e)
            {
                e.printStackTrace();
                logger.error(String.format("{%s【%s】}  errroinfo : %s", sellerDo.getId(), sellerDo.getNick(),
                        Arrays.toString(e.getStackTrace())));
                continue;
            }
        }
*/
    }

    private boolean filterBySellerId(SellerDo sellerDo)
    {
        long id = sellerDo.getId();
//        return id == 94399436L;
//        return id == 622456452;
        return id == 866936997L || id == 94399436L || id == 2057765083L || id == 2223768392L;
//        	|| id == 331869718l || id == 937825776l || id == 387947922l || id == 66537413l || id == 35643535l || id == 792945115
//                || id == 903132895l || id == 776645529l || id == 23261374l || id == 175473761l || id == 913099165l
//                || id == 2078085190l;
    }

    public void executeBuyerInfo(Long sellerId)
    {
    	logger.info("sellerId :------------" + sellerId);
        long time = System.currentTimeMillis();
        SellerDo sellerDo = productBo.getSellerDoById(sellerId);
        if (sellerDo == null)
        {
            throw new IllegalArgumentException(" seller can't be null . " + sellerId);
        }
        try
        {
            AppParameterTaobao parameterTaobao = getParameters(sellerId);
            if (parameterTaobao == null)
            {
                TimeUnit.MILLISECONDS.sleep(500);
                parameterTaobao = getParameters(sellerId);
                if (parameterTaobao == null)
                {
                    return;
                }
            }
            PageQuery pageQuery = new PageQuery(0, 5000);
            while (true)
            {
                long start = System.currentTimeMillis();
                PageList<BuyerDo> list = buyerDao.listAllByInfo(sellerId, pageQuery);
                if (list == null || list.isEmpty())
                {
                    break;
                }
                int i = 0;
                logger.info("list size :------------" + list.size());
                for (BuyerDo buyerDo : list)
                {
                    if (isPass(buyerDo))
                    {
                        ++i;
                        continue;
                    } else
                    {
                        if (logger.isInfoEnabled())
                        {
                            logger.info(String.format(
                                    "[%s-%s-%s] info time: %s ,update time : %s create time : %s ,last login time : %s",
                                    sellerDo.getId(), sellerDo.getNick(), buyerDo.getNick(),
                                    DateUtils.formatDate(buyerDo.getLastUpdateinfTime()),
                                    DateUtils.formatDate(buyerDo.getGmtModified()), DateUtils.formatDate(buyerDo.getCreated()),
                                    DateUtils.formatDate(buyerDo.getLastLoginTime())));
                        }
                        executeBuyerGradeAndInfo(parameterTaobao, sellerId, buyerDo);
                    }

                }
                Paginator paginator = list.getPaginator();
                if (logger.isInfoEnabled())
                {
                    logger.info(String
                            .format("{%s【%s】}  \t \n >>> \u540c\u6b65\u4f1a\u5458\u4fe1\u606f \u5171(%s)\u6761,index(%s),\u5f53\u524d\u4f1a\u5458\u6761\u6570(%s),\u8df3\u8fc7(%s)\u6761,time(%s)-ms \t \n ",
                                    sellerDo.getId(), sellerDo.getNick(), paginator.getItems(), pageQuery.getStartIndex(),
                                    list.size(), i, (System.currentTimeMillis() - start)));
                }
                if (paginator.getNextPage() == paginator.getPage())
                {
                    break;
                }
                pageQuery.increasePageNum();
            }

            if (logger.isInfoEnabled())
            {
                logger.info(String.format("{%s【%s】} \u4f1a\u5458\u4fe1\u606f\u66f4\u65b0\u5b8c\u6210. times( %s ) -ms",
                        sellerDo.getId(), sellerDo.getNick(), (System.currentTimeMillis() - time)));
            }

        } catch (Exception e)
        {
            e.printStackTrace();
            logger.error("{" + sellerId + "【" + sellerDo.getNick() + " 】}#syncBuyerInfo error info :# "
                    + Arrays.toString(e.getStackTrace()));
        } finally
        {
            FLAG_MAP.get(sellerId).set(Boolean.TRUE);
        }

    }

    private void executeBuyerGradeAndInfo(AppParameterTaobao parameterTaobao, Long sellerId, BuyerDo buyerDo)
    {
        //        long start = System.currentTimeMillis();

        try
        {
            sellerService.getTaobaoBuyerGrade(getTaobaoClient(parameterTaobao), parameterTaobao, buyerDo);
        } catch (Exception e)
        {
            e.printStackTrace();
            logger.error(buyerDo.getNick() + " === Sync Taobao Buyer grade error is :"
                    + Arrays.toString(e.getStackTrace()));
        }

        try
        {
            sellerService.getTaobaoBuyerInfo(getTaobaoClient(parameterTaobao), parameterTaobao, buyerDo);
        } catch (Exception e)
        {
            e.printStackTrace();
            logger.error(buyerDo.getNick() + " === Sync Taobao Buyer info error is :"
                    + Arrays.toString(e.getStackTrace()));
        }

        updateInfTime(buyerDo);

        //        if (logger.isInfoEnabled()) {
        //            logger.info(String.format("【 %s 】execute over times ( %s ) -ms . ", buyerDo.getNick(),
        //                    (System.currentTimeMillis() - start)));
        //        }
    }

    private void updateInfTime(BuyerDo buyerDo)
    {
        BuyerDo updateBuyer = new BuyerDo();
        updateBuyer.setId(buyerDo.getId());
        updateBuyer.setSellerId(buyerDo.getSellerId());
        updateBuyer.setLastUpdateinfTime(DateUtils.getNextSecondForDate(DateUtils.now(), 60));
        buyerDao.update(updateBuyer);
    }

    private AppParameterTaobao getParameters(Long sellerId)
    {

        AppParameterTaobao p = null;
        List<SellerProductSubscribeDo> products = sellerProductSubscribeDao.listBySellerId(sellerId);
        if (products == null || products.isEmpty())
        {
            return p;
        }
        for (SellerProductSubscribeDo sellerProductSubscribeDo : products)
        {
            if (sellerProductSubscribeDo.getProductId() != null
                    && sellerProductSubscribeDo.getProductId().intValue() == ProductType.CRM.getValue())
            {
                if (StrUtils.isNotEmpty(sellerProductSubscribeDo.getParameter()))
                {
                    AppParameterTaobao sellerTaobaoParameter = StrUtils.getGson().fromJson(sellerProductSubscribeDo.getParameter(),
                            AppParameterTaobao.class);
                    if (sellerTaobaoParameter != null)
                    {
                        String outTime = sellerTaobaoParameter.getExpiresIn();
                        if (StrUtils.isNotEmpty(outTime))
                        {
                            long time = Long.parseLong(outTime);
                            long now = DateUtils.now().getTime();
                            if (time > now)
                            {
                                p = sellerTaobaoParameter;
                                break;
                            }
                        }
                    }
                }
            }
        }
        return p;
    }

    /**
     * @param buyerDo
     * @return
     */
    private boolean isPass(BuyerDo buyerDo)
    {
        if (buyerDo.getLastUpdateinfTime() != null && buyerDo.getGmtModified() != null)
        {
            boolean isNewTrade = buyerDo.getLastUpdateinfTime().getTime() >= buyerDo.getGmtModified().getTime(); //会员没有新订单
            boolean isUpdate = buyerDo.getLastUpdateinfTime().after(DateUtils.getNextDays(new Date(), -8));
            if (isNewTrade || isUpdate)
            {
                if (buyerDo.getLastLoginTime() != null || buyerDo.getCreated() != null)
                {
                    return Boolean.TRUE;
                }
            }
        }
    	return Boolean.FALSE;
    }

    /**
     * 判断 session key 是否在有效期内
     * 
     * @param sellerDo
     * @return
     */
    private boolean checkSessionKey(SellerDo sellerDo)
    {
        try
        {
            if (sellerDo.getSessionkeyExpires() != null && sellerDo.getSessionkeyExpires().intValue() == 0)
            {
                return true;
            } else
            {
                return getParameters(sellerDo.getId()) != null;
            }
        } catch (Exception e)
        {
            e.printStackTrace();
            logger.error(" checkSessionKey error info : " + Arrays.toString(e.getStackTrace()));
            return Boolean.FALSE;
        }
    }

    private ThreadPoolExecutor initThreadPool(int size)
    {
        return new ThreadPoolExecutor(size, size, 100, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
                new ThreadPoolExecutor.AbortPolicy());
    }

    private TaobaoClient getTaobaoClient(AppParameterTaobao parameterTaobao)
    {
        return new DefaultTaobaoClient(clientURL, parameterTaobao.getAppkey(), parameterTaobao.getSecret(), FORMAT_JSON);
    }

    private static final String clientURL   = "http://gw.api.taobao.com/router/rest";

    private static final String FORMAT_JSON = "json";

    public static void main(String[] args)
    {
        for (int i = 0; i < 20; i++)
        {
            System.out.println(i % 7);
        }

    }
}
