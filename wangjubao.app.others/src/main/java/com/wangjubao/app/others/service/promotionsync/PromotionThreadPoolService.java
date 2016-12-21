package com.wangjubao.app.others.service.promotionsync;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.app.others.service.SellerInfoMap;

@Service
public class PromotionThreadPoolService {

	protected transient final static Logger logger = Logger.getLogger("promotionsync");
	
	 private ThreadPoolExecutor                sellerThreadPool;
	    private Map<String, Boolean>                       runnableSellers = new ConcurrentHashMap<String, Boolean>();
	    private Set<String>                       shouldRunSellers = new HashSet<String>();
	    private Set<String>                       addupSellers = new HashSet<String>();
	    private Set<String>                       subtractSellers = new HashSet<String>();
	    private Long                       			lastSellerCheckTime = 0L;
	     @Autowired
	    SellerInfoMap sellerInfoMap;
	    /**
	     * 卖家外部原始数据抓取器的线程池守护任务会定时检查卖家是否有增减
	     * 这个时限代表了多久检查一次
	     */
	    public static final Long CKECK_TIME_LIMIT = 60000L;
	    
	    /**
	     * 卖家线程池的线程数上限
	     * 由于线程池的线程数会根据卖家数量动态变化，
	     * 但是线程数过多反而降低性能，
	     * 所以设定此值规定卖家线程池线程数的最大上限
	     */
	    public static final int MAX_THREAD_COUNT = 1;
	    /**
	     * 默认线程池设置，在使用线程池时可以动态根据需求调整
	     */
	    public static final int THREAD_POOL_DEFAULT_CORE_POOL_SIZE = 1;
	    public static final int THREAD_POOL_DEFAULT_MAXIMUM_POOL_SIZE = 1;
	    public static final int THREAD_POOL_DEFAULT_KEEP_ALIVE_TIME = 5;
	    public static final TimeUnit THREAD_POOL_DEFAULT_KEEP_ALIVE_TIME_UNIT = TimeUnit.SECONDS;
	    public static final Integer THREAD_POOL_DEFAULT_WAIT_QUEUE_SIZE = Integer.MAX_VALUE;
	    
	    @Autowired
	    private SellerInfoMap                             sellerInfoBySellerNick;
	    
	    @Autowired
	    private PromotionSyncTask promotionSyncTask;
	    
	public void startTaskInThreadPool() {

        configureThreadPool();
		startTaskBySellerNickInThreadPool();

    }

	private void startTaskBySellerNickInThreadPool() {
//		logger.info("----------------runnableSellers start: ");
        synchronized (runnableSellers)
        {
//	        adjustRunnableSellers();
	        if (logger.isInfoEnabled())
	        {
//	        	logger.info("----------------sellerInfoMap size====="+sellerInfoMap.size());
	        }
            for (Iterator<String> iterator = sellerInfoMap.getSellerNicks().iterator(); iterator.hasNext();)
            {
                String sellerNick = iterator.next();
                if(runnableSellers.get(sellerNick)!=null&&runnableSellers.get(sellerNick)==true)
                {
        	        if (logger.isInfoEnabled())
        	        {
//        	        	logger.info(" continue-----------------" 
//        	        			+ "sellerNick: " + sellerNick);
        	        }
                	continue;
                }
                PromotionSyncTask promotionSyncTask = new PromotionSyncTask(sellerNick,runnableSellers);
                sellerThreadPool.execute(promotionSyncTask);
                runnableSellers.put(sellerNick, true);
    	        if (logger.isInfoEnabled())
    	        {
//    	        	logger.info(" ok-----------------" 
//    	        			+ "sellerNick: " + sellerNick + "runnableSellers" + runnableSellers);
    	        }
            }
	        if (logger.isInfoEnabled())
	        {
//	        	logger.info("----------------startTaskBySellerNickInThreadPool end");
	        }
        }
        if (logger.isInfoEnabled())
        {
//        	logger.info(" ok-----------------" );
        }
    }

	private void adjustRunnableSellers() {
    	if(addupSellers.size() != 0 || subtractSellers.size() != 0)
    	{
	    	Iterator<String> iteratorForAdd = addupSellers.iterator();
	    	while (iteratorForAdd.hasNext())
	        {
	        	String sellerNick = iteratorForAdd.next();
	        	if(!runnableSellers.containsKey(sellerNick))
	        	{
	        		runnableSellers.put(sellerNick, false);
	        	}
	        }
	    	
	    	Iterator<String> iteratorForSubtract = subtractSellers.iterator();
	    	while (iteratorForSubtract.hasNext())
	        {
	        	String sellerNick = iteratorForSubtract.next();
	        	if(runnableSellers.containsKey(sellerNick))
	        	{
	        		runnableSellers.remove(sellerNick);
	        	}
	        }
    	}
    }

	private void configureThreadPool() {
//    	if(isSellerCheckTimeAlreadyCome() && isSellerChanged())
//    	{
    		sellerThreadPool= initThreadPool();
//	        int sellerCount = sellerInfoBySellerNick.getSellerNicksFORTB().size();
//	        int minSellerThradCount = sellerCount/3;
//	        if(minSellerThradCount == 0)
//	        {
//	        	minSellerThradCount = sellerCount;
//	        }
//	        int corePoolSize = 
//	        		minSellerThradCount > MAX_THREAD_COUNT?MAX_THREAD_COUNT:minSellerThradCount;
//	        int maxPoolsize = corePoolSize;
//	        sellerThreadPool.setCorePoolSize(corePoolSize);
//	        sellerThreadPool.setMaximumPoolSize(maxPoolsize);
//	        if (logger.isInfoEnabled())
//	        {
//	        	logger.info(" ok-----------------" 
//	        			+ "sellerCount: " + sellerCount + " | corePoolSize: " + corePoolSize + "maxPoolsize:" + maxPoolsize);
//	        }
//    	}
    }

	private ThreadPoolExecutor initThreadPool() {
    	if(sellerThreadPool == null)
    	{
	        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(THREAD_POOL_DEFAULT_CORE_POOL_SIZE,
	                THREAD_POOL_DEFAULT_MAXIMUM_POOL_SIZE, THREAD_POOL_DEFAULT_KEEP_ALIVE_TIME,
	                THREAD_POOL_DEFAULT_KEEP_ALIVE_TIME_UNIT, new LinkedBlockingQueue<Runnable>(THREAD_POOL_DEFAULT_WAIT_QUEUE_SIZE));
	        return threadPoolExecutor;
    	}
    	else
    	{
    		return sellerThreadPool;
    	}
    }

	private boolean isSellerChanged() {
    	Set<String> currentRunnableSellers = new HashSet<String>(sellerInfoBySellerNick.getSellerNicksFORTB());
    	if(shouldRunSellers.equals(currentRunnableSellers))
    	{
	        if (logger.isInfoEnabled())
	        {
	        	logger.info("--------------------shouldrunSellers.equals(currentRunnableSellers)" 
	        			+ "currentRunnableSellers.size(): " + currentRunnableSellers.size() + " | runnableSellers.size(): " + runnableSellers.size());
	        }
	        return false;
    	}
    	else
    	{
    		Set<String> runnableSellersForAddup = new HashSet<String>(sellerInfoBySellerNick.getSellerNicksFORTB());
    		runnableSellersForAddup.removeAll(shouldRunSellers);
    		addupSellers = runnableSellersForAddup;
	        if (logger.isInfoEnabled())
	        {
	        	logger.info("--------------------" 
	        			+ "addupSellers: " + addupSellers + "currentRunnableSellers: " + currentRunnableSellers + "shouldRunSellers: " + shouldRunSellers);
	        }
	        HashSet<String> shouldRunSellersClone = cloneHashSet(shouldRunSellers);
	        shouldRunSellersClone.removeAll(currentRunnableSellers);
	        subtractSellers = shouldRunSellersClone;
	        if (logger.isInfoEnabled())
	        {
	        	logger.info("--------------------" 
	        			+ "subtractSellers: " + subtractSellers + "currentRunnableSellers: " + currentRunnableSellers + "shouldRunSellers: " + shouldRunSellers);
	        }
	    	shouldRunSellers = currentRunnableSellers;
	        return true;
    	}
    }

	private HashSet<String> cloneHashSet(Set<String> src) {
    	HashSet<String> dst = new HashSet<String>();
    	Iterator<String> iterator = src.iterator();
    	while (iterator.hasNext())
        {
        	String sellerNick = iterator.next();
        	String cloneSellerNick = new String(sellerNick);
        	dst.add(cloneSellerNick);
        }
        return dst;
    }

	private boolean isSellerCheckTimeAlreadyCome() {
    	Long currenttime= System.currentTimeMillis();
    	Long timePassed = currenttime - lastSellerCheckTime;
    	if(timePassed >= CKECK_TIME_LIMIT)
    	{
    		lastSellerCheckTime = currenttime;
            if (logger.isInfoEnabled())
            {
//            	logger.info(" ok-----------------" +  "isSellerCheckTimeAlreadyCome: true");
            }
    		return true;
    	}
    	else
    	{
            if (logger.isInfoEnabled())
            {
//            	logger.info(" ok-----------------" +  "isSellerCheckTimeAlreadyCome: false");
            }
    		return false;
    	}
    }
}
