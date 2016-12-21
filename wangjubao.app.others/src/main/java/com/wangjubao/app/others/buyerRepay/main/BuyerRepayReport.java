package com.wangjubao.app.others.buyerRepay.main;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.wangjubao.app.others.consume.ConsumeCalculate;
import com.wangjubao.app.others.service.buyerRepay.BuyerRepayReportService;
import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.biz.common.constant.BaseType;
import com.wangjubao.dolphin.biz.common.constant.SellerType;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dao.BuyerRepayDao;
import com.wangjubao.dolphin.biz.dao.SellerDao;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.common.lang.Paginator;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.date.DateUtils;
import com.wangjubao.framework.util.DateUtil;
import com.wangjubao.tool.email.service.EmailSendService;
import com.wangjubao.tool.email.service.impl.EmailSendServiceImpl;

public class BuyerRepayReport {
    static final Logger                               logger           = Logger.getLogger("histroyimport");

    private BuyerRepayDao                             buyerRepayDao;

    private SellerDao                                 sellerDao;

    private BuyerRepayReportService                   buyerRepayReportService;

    private static Map<String, List<SellerDo>>        sellerMap        = new ConcurrentHashMap<String, List<SellerDo>>();
    //存放一个线程map
    private static final Map<String, ExecutorService> EXECUTOR_MAP     = new ConcurrentHashMap<String, ExecutorService>();

    private static final Map<String, Boolean>         FLAG_MAP         = new ConcurrentHashMap<String, Boolean>();

    private static EmailSendService                   emailSendService = new EmailSendServiceImpl();

    private static String[]                           emails           = new String[] { "tech@wangjubao.com" };

    public BuyerRepayReport() {
        sellerDao = (SellerDao) SynContext.getObject("sellerDao");
        buyerRepayDao = (BuyerRepayDao) SynContext.getObject("buyerRepayDao");
        buyerRepayReportService = (BuyerRepayReportService) SynContext
                .getObject("buyerRepayReportService");
    }

    public void init() {
        // TODO Auto-generated method stub
        //        BuyerRepayDao.resetStatusToWait(null);
    }

    /**
     * 费用计算主线程
     */
    public void doDataFetch() {
        new Thread() {
            public void run() {
                while (true) {
                    //凌晨1点到5点执行
                    String tstart = "20000";
                    Integer endt = Integer.parseInt(tstart) + 150000;
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

                    Map<String, List<SellerDo>> map = builderSellerMap();
                    Set<Entry<String, List<SellerDo>>> list = map.entrySet();

                    for (Iterator<Entry<String, List<SellerDo>>> it = list.iterator(); it.hasNext();) {
                        Entry<String, List<SellerDo>> entry = it.next();

                        /*
                         * ①每个数据源一个线程（线程池） doCalculateThread(entry);
                         */

                        //②根据flag来判定该卖家是否属于同个数据源，同个的话该线程就wait，不同的话就执行
                        final List<SellerDo> lists = entry.getValue();
                        final String key = entry.getKey();
                        if (!FLAG_MAP.containsKey(key)) {
                            FLAG_MAP.put(key, Boolean.TRUE);
                        }
                        if (FLAG_MAP.get(key)) {
                            FLAG_MAP.put(key, Boolean.FALSE);
                            ExecutorService thread = EXECUTOR_MAP.get(entry.getKey());
                            if (thread == null) {
                                thread = Executors.newSingleThreadExecutor();
                                EXECUTOR_MAP.put(entry.getKey(), thread);
                            }

                            thread.execute(new Runnable() {

                                @Override
                                public void run() {

                                    try {

                                        Iterator<SellerDo> sellerit = lists.iterator();
                                        while (sellerit.hasNext()) {
                                            final SellerDo sellerDo = sellerit.next();
                                            if (sellerDo != null 
                                                    && sellerDo.getSellerType() != null
                                                    && (sellerDo.getSellerType().intValue() == 1 || sellerDo
                                                            .getSellerType().intValue() == 8)) {
                                                
                                                if (StringUtils.equalsIgnoreCase(key,
                                                        sellerDo.getDatasourceKey())) {
                                                	//101450072，1049653664，43779244这几个店铺数据量特别大，所以进行了过滤。只计算淘宝天猫类型的店铺
                                                    if (sellerDo.getId() != 1049653664
                                                            && sellerDo.getId() != 101450072
                                                            && sellerDo.getId() != 43779244l ) {
                                                        logger.info(String
                                                                .format(" ################################### 淘宝平台 %s,%s,%s",
                                                                        sellerDo.getId(),
                                                                        sellerDo.getNick(),
                                                                        sellerDo.getDatasourceKey()));
                                                        doCalculate(sellerDo);
                                                    }
                                                } else {
                                                    throw new IllegalArgumentException(
                                                            " datasource key " + key + " seller :"
                                                                    + sellerDo.toString());
                                                }
                                            } 
                                            try {
                                                sleep(1000);
                                            } catch (InterruptedException e) {
                                                e.printStackTrace();
                                            }
                                        }

                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    } finally {
                                        FLAG_MAP.put(key, Boolean.TRUE);
                                    }
                                }

                            });
                        }
                    }

                    try {
                        sleep(1000 * 60 * 60);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }
            }
        }.start();
    }

    /**
     * 按店铺计算对应的每人每个商品的回购数据
     * 
     * @throws ParseException
     */
    private void doCalculate(SellerDo sellerDo) throws ParseException {
        if (sellerDo != null && sellerDo.getSourceId() != null) {
            long start = System.currentTimeMillis();
            logger.info(String.format("{%s【%s】} begin anaBuyerRepayReport, time : 【%s】\t",
                    sellerDo.getId(), sellerDo.getNick(), DateUtils.formatDate(DateUtils.now())));

            try {
                buyerRepayReportService.anaBuyerRepayReport(sellerDo.getId(), null);
            } catch (Exception e) {
                e.printStackTrace();
                //                SellerDo sellerDoo = new SellerDo();
                //                sellerDoo.setId(sellerDo.getId());
                //                sellerDoo.setRefreshBuyerday(BaseType.CALCULATE_FAILED);
                //                sellerDao.updateRefresh(sellerDo);
                logger.error(String.format("XXXXXXXXXXXXXXX 【%s】（%s）回购周期计算失败。 error info >  ",
                        sellerDo.getId(), Arrays.toString(e.getStackTrace())));
                String specialInformation = "seller id : " + sellerDo.getId();
                emailSendService.emailSend(" 回购周期计算失败 ", emails, specialInformation, e);
            }
            logger.info(String.format(
                    "{%s【%s】} anaBuyerRepayReport finished（%s）-ms | time : 【%s】\t \n",
                    sellerDo.getId(), sellerDo.getNick(), (System.currentTimeMillis() - start),
                    DateUtils.formatDate(DateUtils.now())));
        }
    }

    /**
     * 获取所有店铺，并按照数据源分类
     */
    private Map<String, List<SellerDo>> builderSellerMap() {
        Map<String, List<SellerDo>> cMap = new ConcurrentHashMap<String, List<SellerDo>>();
        List<SellerDo> sellers = sellerDao.listActiveCompanySeller();
/*        List<SellerDo> sellers = new ArrayList<SellerDo>();
        SellerDo seller1 = sellerDao.load(2242695961L);
        sellers.add(seller1);*/
        for(SellerDo seller : sellers){
        	if (StringUtils.isBlank(seller.getDatasourceKey())||SellerType.QIANNIU==seller.getSellerType().intValue()) {
                continue;
            }
        	if(seller.getId().longValue() != 2057428013L)
        		continue;
        	List<SellerDo> sellerlist = sellerMap.get(seller.getDatasourceKey());
            if (sellerlist == null) {
                sellerlist = new ArrayList<SellerDo>();
            } else {
                Iterator<SellerDo> iterator = sellerlist.iterator();
                while (iterator.hasNext()) {
                    SellerDo sellerDo = iterator.next();
                    if (sellerDo.getId().longValue() == seller.getId().longValue()) {
                        iterator.remove();
                        //                            sellerlist.remove(sellerDo);
                        break;
                    }
                }
            }
            sellerlist.add(seller);
            sellerMap.put(seller.getDatasourceKey(), sellerlist);
        }
        /*
        PageQuery pageQuery = new PageQuery(0, 4000);
        while (true) {

            PageList<SellerDo> resultDo = sellerDao.listAll(null, null, null, pageQuery);

            if (resultDo == null) {
                break;
            }
            for (SellerDo s : resultDo) {
                if (StringUtils.isBlank(s.getDatasourceKey())||SellerType.QIANNIU==s.getSellerType().intValue()) {
                    continue;
                }
                List<SellerDo> sellerlist = sellerMap.get(s.getDatasourceKey());
                if (sellerlist == null) {
                    sellerlist = new ArrayList<SellerDo>();
                } else {
                    Iterator<SellerDo> iterator = sellerlist.iterator();
                    while (iterator.hasNext()) {
                        SellerDo sellerDo = iterator.next();
                        if (sellerDo.getId().longValue() == s.getId().longValue()) {
                            iterator.remove();
                            //                            sellerlist.remove(sellerDo);
                            break;
                        }
                    }
                }
                sellerlist.add(s);
                sellerMap.put(s.getDatasourceKey(), sellerlist);

            }
            Paginator paginator = resultDo.getPaginator();
            if (paginator.getNextPage() == paginator.getPage()) {
                break;
            }
            pageQuery.increasePageNum();
        }
        */
        cMap.putAll(sellerMap);
        return cMap;
    }
}
