package com.wangjubao.app.others.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.dolphin.biz.common.constant.SellerType;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dao.SellerDao;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.common.lang.Paginator;
import com.wangjubao.dolphin.common.util.PageList;

@Service
public class SellerInfoMap {

    static final Logger                logger    = Logger.getLogger("authcheck");

    public static final AtomicBoolean  INIT      = new AtomicBoolean(Boolean.FALSE);

    @Autowired
    private SellerDao                  sellerDao;

    public Map<String, List<SellerDo>> sellerMap = new ConcurrentHashMap<String, List<SellerDo>>();
    
    public Map<String, SellerDo>           sellerNickAndInfoMap = new ConcurrentHashMap<String, SellerDo>();

    // 默认会取得一次所有卖家信息，以便于每个线程使用卖家信息时都能有数据
    @PostConstruct
    public void loadAllSellerDataAndSave()
    {
        synchronized (this)
        {
            builderSellerMap();
            if (logger.isInfoEnabled())
            {
                logger.info("初始化卖家信息,安数据源分配-----------------" + "sellerMap: " + sellerMap.size());
            }
            INIT.set(Boolean.TRUE);
        }

    }

    /**
     * 获取所有店铺，并按照数据源分类
     */
    private Map<String, List<SellerDo>> builderSellerMap()
    {
//        Map<String, List<SellerDo>> cMap = new ConcurrentHashMap<String, List<SellerDo>>();
            ////
            List<SellerDo> resultDo = sellerDao.listActiveCompanySeller();

            if (resultDo == null)
            {
                return null;
            }
            for (SellerDo s : resultDo)
            {
                if (StringUtils.isBlank(s.getDrdsSourceKey()))
                {
                    continue;
                }
                if ( SellerType.TAOBAO != s.getSellerType().intValue() && SellerType.TMALL!=s.getSellerType().intValue()) {
					continue;
				}
                
                List<SellerDo> sellerlist = sellerMap.get(s.getDrdsSourceKey());
                if (sellerlist == null)
                {
                    sellerlist = new ArrayList<SellerDo>();
                } else
                {
                    Iterator<SellerDo> iterator = sellerlist.iterator();
                    while (iterator.hasNext())
                    {
                        SellerDo sellerDo = iterator.next();
                        if (sellerDo.getId().longValue() == s.getId().longValue())
                        {
                            iterator.remove();
                            //                            sellerlist.remove(sellerDo);
                            break;
                        }
                    }
                }
                sellerlist.add(s);
                sellerMap.put(s.getDrdsSourceKey(), sellerlist);
                
                SellerDo oldDo = sellerNickAndInfoMap.get(s.getNick());
                if (oldDo == null) {
                	sellerNickAndInfoMap.put(s.getNick(), s);
				}

            }
//        cMap.putAll(sellerMap);
        return sellerMap;
    }

    public Map<String, List<SellerDo>> getSellerMap()
    {
        synchronized (sellerMap)
        {
            Map<String, List<SellerDo>> newSellerMap = new ConcurrentHashMap<String, List<SellerDo>>();
            for (Iterator<String> iterator = sellerMap.keySet().iterator(); iterator.hasNext();)
            {
                String key = (String) iterator.next();
                List<SellerDo> value = sellerMap.get(key);
                newSellerMap.put(key, value);
            }
            return newSellerMap;
        }
    }

    public int size()
    {
        synchronized (sellerMap)
        {
            return sellerMap.size();
        }
    }
    
    public Set<String> getSellerNicks()
    {
        synchronized (sellerNickAndInfoMap)
        {
            Set<String> sellerNickSet = new HashSet<String>();
            for (Iterator<String> iterator = sellerNickAndInfoMap.keySet().iterator(); iterator.hasNext();)
            {
            	String sellerNick = iterator.next();
            	String cloneSellerNick = new String(sellerNick);
            	sellerNickSet.add(cloneSellerNick);
            }
            return sellerNickSet;
        }
    }
    
    public Set<String> getSellerNicksFORTB()
    {
        synchronized (sellerNickAndInfoMap)
        {
            Set<String> sellerNickSet = new HashSet<String>();
            for (Iterator<String> iterator = sellerNickAndInfoMap.keySet().iterator(); iterator.hasNext();)
            {
            	String sellerNick = iterator.next();
            	SellerDo s = sellerNickAndInfoMap.get(sellerNick);
            	if ( SellerType.TAOBAO != s.getSellerType().intValue() && SellerType.TMALL!=s.getSellerType().intValue()) {
					continue;
				}
            	String cloneSellerNick = new String(sellerNick);
            	sellerNickSet.add(cloneSellerNick);
            }
            return sellerNickSet;
        }
    }

    public static void main(String[] args) {
		
    	 if ( SellerType.TAOBAO != 2 && SellerType.TMALL!=2) {
				System.out.println(212);
			}	
	}
}
