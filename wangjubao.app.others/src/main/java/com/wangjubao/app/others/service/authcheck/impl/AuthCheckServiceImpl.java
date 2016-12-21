package com.wangjubao.app.others.service.authcheck.impl;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.app.others.service.SellerInfoMap;
import com.wangjubao.app.others.service.authcheck.AuthCheckService;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.biz.model.extend.AppParameterTaobao;
import com.wangjubao.dolphin.biz.service.SellerSessionKeyService;
import com.wangjubao.dolphin.biz.service.WjbApiClientService;
import com.wangjubao.dolphin.common.util.StrUtils;
import com.wangjubao.dolphin.common.util.date.DateUtils;

@Service("authCheckService")
public class AuthCheckServiceImpl implements AuthCheckService {
    static final Logger                               logger       = Logger.getLogger("authcheck");

    //存放一个线程map
    private static final Map<String, ExecutorService> EXECUTOR_MAP = new ConcurrentHashMap<String, ExecutorService>();

    private static final Map<String, Boolean>         FLAG_MAP     = new ConcurrentHashMap<String, Boolean>();

    @Autowired
    private WjbApiClientService                       wjbApiClientService;

    @Autowired
    private SellerSessionKeyService                   sellerSessionKeyService;

    @Autowired
    private SellerInfoMap                             sellerInfoMap;

    @Override
    public synchronized void check()
    {
        logger.info(" ####################################### 授权检查启动..." + DateUtils.formatDate(DateUtils.now()));
        while (!SellerInfoMap.INIT.get())
        {
            logger.info(" wait seller init ... ");
            Thread.yield();
        }
        Iterator<Entry<String, List<SellerDo>>> iterator = sellerInfoMap.getSellerMap().entrySet().iterator();
        while (iterator.hasNext())
        {
            Entry<String, List<SellerDo>> entry = iterator.next();
            final List<SellerDo> lists = entry.getValue();
            final String key = entry.getKey();
            if (!FLAG_MAP.containsKey(key))
            {
                FLAG_MAP.put(key, Boolean.TRUE);
            }
            if (FLAG_MAP.get(key))
            {
                FLAG_MAP.put(key, Boolean.FALSE);
                ExecutorService thread = EXECUTOR_MAP.get(entry.getKey());
                if (thread == null)
                {
                    thread = Executors.newSingleThreadExecutor();
                    EXECUTOR_MAP.put(entry.getKey(), thread);
                }

                thread.execute(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {

                            Iterator<SellerDo> sellerit = lists.iterator();
                            Integer size = lists.size();
                            while (sellerit.hasNext())
                            {
                                //                                    	logger.info("长度++++++++++++++++++++++++++"+sellerInfoMap.getSellerMap().size());
                                final SellerDo sellerDo = sellerit.next();
                                if (sellerDo != null)
                                {
                                    //                                           if(sellerDo.getId()==331869718){
                                    checkAuth(sellerDo);
                                    size = size - 1;
                                    //                                           }
                                }
                                logger.info(String.format(" 【%s】执行完成########################### 数据源%s,剩余【%s】个要执行", sellerDo.getNick(),
                                        key, size));
                            }
                        } catch (Exception e)
                        {
                            logger.info(StrUtils.showError(e));
                            e.printStackTrace();
                        } finally
                        {
                            FLAG_MAP.put(key, Boolean.TRUE);
                        }
                    }

                });
            }
        }
    }

    @PostConstruct
    @Override
    public void init()
    {

    }

    private void checkAuth(SellerDo sellerDo)
    {
        try
        {
            if (this.sellerSessionKeyService.sessionReview(sellerDo.getId()))
            {

            } else
            {
                logger.info(String.format("(%s)[%s]卖家授权检查失败，需要重新授权", sellerDo.getNick(), sellerDo.getId()));
                updateExpireDate(sellerDo);
            }
        } catch (Exception e)
        {
            logger.info(String.format("(%s)[%s]卖家授权检查失败，需要重新授权", sellerDo.getNick(), sellerDo.getId()));
            updateExpireDate(sellerDo);
        }

    }

    private void updateExpireDate(SellerDo sellerDo)
    {
        AppParameterTaobao product = this.sellerSessionKeyService.getSellerParameterBySourceIdAndSellerType(sellerDo.getId() + "",
                sellerDo.getSellerType(), 1);
        String wjb_expire_date = null;
        if (product != null && StrUtils.isNotEmpty(product.getExpiresIn()))
        {
            Long expiresIn = Long.valueOf(product.getExpiresIn());
            wjb_expire_date = DateUtils.formatDate(new Date(expiresIn), "yyyy-mm-dd HH:mm:ss");
            if (expiresIn.longValue() > new Date().getTime())
            {
                this.sellerSessionKeyService.updateProductSubscribe(sellerDo.getId(), sellerDo.getSellerType(),
                        DateUtils.nextDays(new Date(), -1));
                wjb_expire_date = DateUtils.formatDate(DateUtils.nextDays(new Date(), -1), "yyyy-mm-dd HH:mm:ss");
            }
        }
        logger.info(String.format("(%s)[%s]卖家授权更新成功，更新到：{%s}", sellerDo.getNick(), sellerDo.getId(),wjb_expire_date));
        this.wjbApiClientService.SellerCorpAuth(sellerDo.getId().toString(), null, null, wjb_expire_date);
        logger.info(String.format("(%s)[%s]卖家授权更新到一折网缓存成功，更新到：{%s}", sellerDo.getNick(), sellerDo.getId(),wjb_expire_date));
    }

}
