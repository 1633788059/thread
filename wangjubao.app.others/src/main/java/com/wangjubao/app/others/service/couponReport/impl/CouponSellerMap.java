package com.wangjubao.app.others.service.couponReport.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;
import com.wangjubao.core.domain.syn.TaobaoUserKey;
import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.biz.common.constant.ProductType;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dao.SellerDao;
import com.wangjubao.dolphin.biz.dao.SellerProductSubscribeDao;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.biz.model.SellerProductSubscribeDo;
import com.wangjubao.dolphin.biz.model.extend.AppParameterTaobao;
import com.wangjubao.dolphin.common.lang.Paginator;
import com.wangjubao.dolphin.common.util.PageList;
import org.apache.commons.lang.StringUtils;


public class CouponSellerMap 
{
    public static boolean hasInitOver = false;
    private static Gson               gson   = new Gson();
    public static Map<String, List<SellerDo>> sellerMap = new ConcurrentHashMap<String, List<SellerDo>>();
    
    public static Map<String, List<SellerDo>> sellerMapTemp = new ConcurrentHashMap<String, List<SellerDo>>();
    
    public static Map<Long, AppParameterTaobao> appParameterTaobaoMap = new ConcurrentHashMap<Long, AppParameterTaobao>();
    
    private SellerDao                                 sellerDao;
    
    private SellerProductSubscribeDao sellerProductSubscribeDao;
    
    public CouponSellerMap()
    {
        sellerDao = (SellerDao) SynContext.getObject("sellerDao");
        sellerProductSubscribeDao = (SellerProductSubscribeDao) SynContext.getObject("sellerProductSubscribeDao");
    }
    
    public void doWork()
    {
        new Thread()
        {
            public void run()
            {

                while (true)
                {
                    sellerMapTemp.clear();
                    PageQuery pageQuery = new PageQuery(0, 1000);
                    while (true) {
            
                        PageList<SellerDo> resultDo = sellerDao.listTaoBaoSellerByPage(null, null, null, pageQuery);
            
                        if (resultDo == null) {
                            break;
                        }
                        for (SellerDo s : resultDo) {
                            if (StringUtils.isBlank(s.getDatasourceKey())) {
                                continue;
                            }
                            List<SellerDo> sellerlist = sellerMapTemp.get(s.getDatasourceKey());
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
                            sellerMapTemp.put(s.getDatasourceKey(), sellerlist);
                            
                            List<SellerProductSubscribeDo> products = sellerProductSubscribeDao.listBySellerId(s.getId()); 
                            if (products !=null && !products.isEmpty()) { 
                                for(SellerProductSubscribeDo sellerProductSubscribeDo : products) { 
                                    if(sellerProductSubscribeDo.getProductId().intValue() ==ProductType.CRM .getValue()) { 
                                        if(sellerProductSubscribeDo.getParameter()==null||"".equals(sellerProductSubscribeDo.getParameter())) {
                                            break;
                                        }
                                        AppParameterTaobao sellerTaobaoParameter =gson.fromJson( sellerProductSubscribeDo.getParameter(),AppParameterTaobao.class); 
                                        if (sellerTaobaoParameter == null) {
                                            break;
                                        }
                                        appParameterTaobaoMap.put(s.getId(), sellerTaobaoParameter);
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
                    sellerMap.clear();
                    // 更新 数据源 对应卖家列表
                    sellerMap.putAll(sellerMapTemp);
                    try
                    {
                        hasInitOver = true;
                        sleep(1000 * 60 * 60 * 12);
                    } catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        }.start();
    }
}
