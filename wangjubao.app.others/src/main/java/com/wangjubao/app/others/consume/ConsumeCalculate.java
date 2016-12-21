package com.wangjubao.app.others.consume;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wangjubao.app.others.dayreport.SellerDayReport;
import com.wangjubao.app.others.service.consume.ConsumeCalculateService;
import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.biz.common.constant.ActivityInfoType;
import com.wangjubao.dolphin.biz.common.constant.ActivityInfoType.ActivityEnum;
import com.wangjubao.dolphin.biz.common.constant.SellerType;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dao.ConsumeLogDao;
import com.wangjubao.dolphin.biz.dao.SellerDao;
import com.wangjubao.dolphin.biz.dao.SellerProductSubscribeDao;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.biz.service.SellerDayReportService;
import com.wangjubao.dolphin.common.lang.Paginator;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.date.DateUtils;
import com.wangjubao.framework.util.DateUtil;

/**
 * 短信邮件每日费用计算
 * 
 * @author luorixin
 */
public class ConsumeCalculate {
	static final Logger logger = LoggerFactory.getLogger("consume");

	private SellerDao sellerDao;

	private ConsumeLogDao consumeLogDao;

	private ConsumeCalculateService consumeCalculateService;

	private static Map<String, List<SellerDo>> sellerMap = new ConcurrentHashMap<String, List<SellerDo>>();

	public static Map<String, String> consumeFrom = new ConcurrentHashMap<String, String>();

	// 存放一个线程map
	private static final Map<String, ExecutorService> EXECUTOR_MAP = new ConcurrentHashMap<String, ExecutorService>();

	private static final Map<String, Boolean> FLAG_MAP = new ConcurrentHashMap<String, Boolean>();

	private static final ScheduledExecutorService mainExecutor = Executors.newScheduledThreadPool(1);

	private static ExecutorService caculateExecutor = Executors.newFixedThreadPool(8);

	private static ConcurrentHashMap<Long, Boolean> tokenMap = new ConcurrentHashMap<Long, Boolean>();

	// 单个数据源上最大的线程数 默认值 該值優先取配置文件中的值，這里沒有多大意義。支持動太修改。
	// private static final int maxThread = 1;
	//
	// private static Map<String, ThreadPoolExecutor> consumeExecutor = new
	// HashMap<String, ThreadPoolExecutor>();
	//
	// private static ThreadPoolExecutor defaultPool = new ThreadPoolExecutor(
	// maxThread,
	// maxThread,
	// 100,
	// TimeUnit.MILLISECONDS,
	// new LinkedBlockingQueue<Runnable>(),
	// new ThreadPoolExecutor.AbortPolicy()); // 表示拒绝任务并抛出异常

	static {
		ActivityEnum[] activitys = ActivityEnum.values();
		for (ActivityEnum activity : activitys) {
				consumeFrom.put(activity.getName(), activity.getDesc());
		}
	}

	public ConsumeCalculate() {
		sellerDao = (SellerDao) SynContext.getObject("sellerDao");
		consumeLogDao = (ConsumeLogDao) SynContext.getObject("consumeLogDao");
		consumeCalculateService = (ConsumeCalculateService) SynContext.getObject("consumeCalculateService");
	}

	public void init() {
		// TODO Auto-generated method stub
		/*
		 * 一个数据源一个线程池 Map<String, List<SellerDo>> map = builderSellerMap(); if
		 * (!map.isEmpty()) { //重置consume_log表中所有进行中，执行失败的状态为等待
		 * consumeLogDao.resetStatusToWait(null); Set<Entry<String,
		 * List<SellerDo>>> list = map.entrySet(); for (Iterator<Entry<String,
		 * List<SellerDo>>> it = list.iterator(); it.hasNext();) { Entry<String,
		 * List<SellerDo>> entry = it.next(); //每个数据源放入线程池中并每个线程池只有一个线程
		 * consumeExecutor.put(entry.getKey(), defaultPool); } }
		 */
		// 重置consume_log表中所有进行中，执行失败的状态为等待
//		consumeLogDao.resetStatusToWait(null);
	}

	/**
	 * 费用计算主线程
	 */
	public void doDataFetch() {
//		doCalculate();
		mainExecutor.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				// 凌晨1点到5点执行
				int hour = DateUtils.nowHour();
				if (hour < 1 || hour > 5)
					return;
				doCalculate();
			}

		}, 5, 5, TimeUnit.HOURS);
	}

	private void doCalculate() {
		List<SellerDo> resultList = sellerDao.listActiveCompanySeller();
		if (resultList == null)
			return;

		for (final SellerDo seller : resultList) {
			if (!this.getToken(seller.getId())) {
				logger.error("[{}], Another task is running, skip", seller.getNick());
				continue;
			}
			caculateExecutor.submit(new Runnable() {
				@Override
				public void run() {
					try {
						doCalculate(seller);
						logger.info(" 【%s】执行完成########################### ", seller.getNick());
						try {
							Thread.sleep(10000);
						} catch (InterruptedException e) {
							Thread.interrupted();
						}
					} catch (Exception e) {
						logger.error("Error when caculate", e);
					} finally {
						releaseToken(seller.getId());
					}
				}
			});
		}

		/*
		 * Map<String, List<SellerDo>> map = builderSellerMap();
		 * Set<Entry<String, List<SellerDo>>> list = map.entrySet();
		 * 
		 * for (Iterator<Entry<String, List<SellerDo>>> it = list.iterator();
		 * it.hasNext();) { Entry<String, List<SellerDo>> entry = it.next();
		 * 
		 * 
		 * //②根据flag来判定该卖家是否属于同个数据源，同个的话该线程就wait，不同的话就执行 final List<SellerDo>
		 * lists = entry.getValue(); final String key = entry.getKey(); if
		 * (!FLAG_MAP.containsKey(key)) { FLAG_MAP.put(key, Boolean.TRUE); } if
		 * (FLAG_MAP.get(key)) { FLAG_MAP.put(key, Boolean.FALSE);
		 * ExecutorService thread = EXECUTOR_MAP.get(entry.getKey()); if (thread
		 * == null) { thread = Executors.newSingleThreadExecutor();
		 * EXECUTOR_MAP.put(entry.getKey(), thread); }
		 * 
		 * thread.execute(new Runnable() {
		 * 
		 * @Override public void run() {
		 * 
		 * try {
		 * 
		 * Iterator<SellerDo> sellerit = lists.iterator(); Integer size =
		 * lists.size(); while (sellerit.hasNext()) { final SellerDo sellerDo =
		 * sellerit.next(); if (sellerDo != null) { //
		 * if(sellerDo.getId()==331869718){ doCalculate(sellerDo); size=size-1;
		 * // } } // if (key.equals("22")) { logger.info(String.format(
		 * " 【%s】执行完成########################### 数据源%s,剩余【%s】个要执行",
		 * sellerDo.getNick(),key, size)); try{ Thread.sleep(10000);
		 * }catch(InterruptedException e){ Thread.interrupted(); } // } }
		 * 
		 * } catch (Exception e) { logger.error("Error when caculate", e); }
		 * finally { FLAG_MAP.put(key, Boolean.TRUE); } }
		 * 
		 * }); } }
		 */
	}

	/**
	 * 按店铺计算对应的每日费用
	 * 
	 * @throws ParseException
	 */
	private void doCalculate(SellerDo sellerDo) throws ParseException {
		if (sellerDo != null && sellerDo.getSourceId() != null) {
			long start = System.currentTimeMillis();
			logger.info(String.format("{%s【%s】} begin Consume_calculate, time : 【%s】\t", sellerDo.getId(),
					sellerDo.getNick(), DateUtils.formatDate(DateUtils.now())));
			consumeLogDao.resetStatusToWait(sellerDo.getId());
			consumeCalculateService.calculateConsumeForDate(sellerDo.getId(), null);

			logger.info(String.format("{%s【%s】} Consume_calculate finished（%s）-ms | time : 【%s】\t \n", sellerDo.getId(),
					sellerDo.getNick(), (System.currentTimeMillis() - start), DateUtils.formatDate(DateUtils.now())));
		}
	}

	/**
	 * 获取所有店铺，并按照数据源分类
	 */
	private Map<String, List<SellerDo>> builderSellerMap() {
		Map<String, List<SellerDo>> cMap = new ConcurrentHashMap<String, List<SellerDo>>();

		List<SellerDo> resultDo = sellerDao.listActiveCompanySeller();
		if (resultDo == null)
			return cMap;

		for (SellerDo s : resultDo) {
			if (StringUtils.isBlank(s.getDrdsSourceKey()) || SellerType.QIANNIU == s.getSellerType().intValue()) {
				continue;
			}
			List<SellerDo> sellerlist = sellerMap.get(s.getDrdsSourceKey());
			if (sellerlist == null) {
				sellerlist = new ArrayList<SellerDo>();
			} else {
				Iterator<SellerDo> iterator = sellerlist.iterator();
				while (iterator.hasNext()) {
					SellerDo sellerDo = iterator.next();
					if (sellerDo.getId().longValue() == s.getId().longValue()) {
						iterator.remove();
						// sellerlist.remove(sellerDo);
						break;
					}
				}
			}
			sellerlist.add(s);
			sellerMap.put(s.getDrdsSourceKey(), sellerlist);
		}

		cMap.putAll(sellerMap);
		return cMap;
	}

	private boolean getToken(Long sellerId) {
		return tokenMap.putIfAbsent(sellerId, Boolean.TRUE) == null;
	}

	private void releaseToken(Long sellerId) {
		tokenMap.remove(sellerId);
	}

	// private void doCalculateThread(final Entry<String, List<SellerDo>> entry)
	// {
	//
	// String sourceKey = entry.getKey();
	// List<SellerDo> sellerlist = entry.getValue();
	// Iterator<SellerDo> sellerit = sellerlist.iterator();
	// while (sellerit.hasNext()) {
	// final SellerDo sellerDo = sellerit.next();
	// if (sellerDo != null) {
	// ThreadPoolExecutor threadPool = consumeExecutor.get(sourceKey);
	// if (threadPool == null) {
	// threadPool = defaultPool;
	// consumeExecutor.put(sourceKey, threadPool);
	// }
	// threadPool.execute(new Runnable() {
	//
	// @Override
	// public void run() {
	// //费用计算方法
	// try {
	// doCalculate(sellerDo);
	// } catch (ParseException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }
	// });
	// }
	// }
	// }

}
