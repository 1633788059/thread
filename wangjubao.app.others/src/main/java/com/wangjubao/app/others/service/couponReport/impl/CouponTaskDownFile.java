package com.wangjubao.app.others.service.couponReport.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.csvreader.CsvReader;
import com.taobao.api.domain.Task;
import com.taobao.api.internal.util.AtsUtils;
import com.wangjubao.app.others.api.service.ApiService;
import com.wangjubao.app.others.service.couponReport.CouponReportService;
import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dao.CouponSendLogDao;
import com.wangjubao.dolphin.biz.dao.CouponTaskDao;
import com.wangjubao.dolphin.biz.model.CouponSendLogDo;
import com.wangjubao.dolphin.biz.model.CouponTaskDo;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.common.lang.Paginator;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.StrUtils;
import com.wangjubao.framework.util.DateUtil;

/**
 * @author luorixin
 * @date 2014-5-23 下午1:51:10
 * @version V1.0
 * @ClassName: CouponReportConsumeTask
 * @Description: 针对任务id获取对应优惠券的详情，同时插入表
 */
// @Service("CouponTaskDownFile")
public class CouponTaskDownFile implements CouponReportService {

	static final Logger logger = Logger.getLogger("couponconsumetask");

	private CouponTaskDao couponTaskDao;

	private ApiService apiService;

	private CouponSendLogDao couponSendLogDao;

	// 存放一个线程map
	private static final Map<String, ExecutorService> EXECUTOR_MAP = new ConcurrentHashMap<String, ExecutorService>();

	private static final Map<String, Boolean> FLAG_MAP = new ConcurrentHashMap<String, Boolean>();

	public static AtomicInteger automic = new AtomicInteger(0);

	public CouponTaskDownFile() {
		couponSendLogDao = (CouponSendLogDao) SynContext
				.getObject("couponSendLogDao");
		couponTaskDao = (CouponTaskDao) SynContext.getObject("couponTaskDao");
		apiService = (ApiService) SynContext.getObject("apiService");
	}

	public static final String zipFile =  "/data/disk1/couponTask/result/";
	public static final String unZipFile = "/data/disk1/couponTask/ungzip/";

	@Override
	public void doWork() {
		new Thread() {
			public void run() {
				while (true) {
					logger.info("******* Down file running *********");
					// 凌晨6点到10点执行
					String tstart = "00000";
					Integer endt = Integer.parseInt(tstart) + 160000;
					String timenow = DateUtil.getTimeStr().replace(":", "");
					boolean timeIsOk = false;
					if (Integer.parseInt(timenow) >= Integer.parseInt(tstart)
							&& Integer.parseInt(timenow) <= endt) {
						timeIsOk = true;
					}

					/*if (false) {
						try {
							sleep(1000 * 60 * 30);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						continue;
					}*/
					// 获取所有卖家
					Map<String, List<SellerDo>> map = CouponSellerMap.sellerMap;

					Set<Entry<String, List<SellerDo>>> list = map.entrySet();

					for (Iterator<Entry<String, List<SellerDo>>> it = list
							.iterator(); it.hasNext();) {
						Entry<String, List<SellerDo>> entry = it.next();

						final List<SellerDo> lists = entry.getValue();
						final String key = entry.getKey();
						try {
							Iterator<SellerDo> sellerit = lists.iterator();
							Integer size = lists.size();
							while (sellerit.hasNext()) {
								final SellerDo sellerDo = sellerit.next();
								/*if(!sellerDo.getId().equals(1110394338l))
                            		continue;*/
								
								if(size%100==0)
									logger.info("Down next Seller >>........................................................................");
								if (CouponTaskParseData.check(sellerDo)) {
									logger.info("Down>>"+sellerDo.getId()+":"+sellerDo.getNick());
									doConsumeTask(sellerDo);
									size--; 
								}
							}

						} catch (Exception e) {
							String error = StrUtils.showError(e);
							logger.error("## Down Data Error 1："+error);
							e.printStackTrace();
						} finally {
							FLAG_MAP.put(key, Boolean.TRUE);
						}

					}

					try {
						logger.info("Down Task Sleeping............2.......................");
						sleep(1000 * 60 * 30);
					} catch (InterruptedException e) {
						String error = StrUtils.showError(e);
						logger.error("## Down Data Error 2："+error);
						e.printStackTrace();
					}

				}
			}
		}.start();

	}

	private void doConsumeTask(SellerDo sellerDo) {
		PageQuery pageQuery = new PageQuery(0, 1000);
		CouponTaskDo couponTaskDo = new CouponTaskDo();
		couponTaskDo.setSellerId(sellerDo.getId());
		ExecutorService executorService = Executors.newFixedThreadPool(5);
		while (true) {
			PageList<CouponTaskDo> pagelist = this.couponTaskDao
					.listPageByTaskStatus(couponTaskDo, pageQuery);
			if (pagelist == null) {
				break;
			}
			for (CouponTaskDo task : pagelist) {
				// 过滤掉已经失败的和成功的，且update时间在60分钟之前的
				logger.info("**** Down taskId["+task.getSellerId()+"] ["+task.getTaskId()+"] memo:"+task.getMemo());
				if ( task.getMemo()==null||(!task.getMemo().equals("fail")
								&& !task.getMemo().equals("done")
								&& !task.getMemo().equals("empty")) ) {
					//if (task.getGmtModified().before(DateUtil.nextMinutes(-60))) {
						Task taskNew = this.apiService.getCouponreport(
								task.getSellerId(), task.getTaskId());
						if(null!=taskNew){
							logger.info("**** Down apiService Status["+task.getSellerId()+"]["+task.getTaskId()+"]:"+taskNew.getStatus());
							if(!"done".equals(taskNew.getStatus()))
								task.setMemo(taskNew.getStatus());							
						}
						this.couponTaskDao.update(task);
						if (taskNew != null && !"empty".equals(taskNew.getStatus())&& !"notask".equals(taskNew.getStatus())) {
							try {
								String url = taskNew.getDownloadUrl();
								if (StrUtils.isNotEmpty(url)) {
									executorService.submit(new TaskDownFile(sellerDo, task, taskNew, couponTaskDao));
								}else{
									logger.info("**** 还没有准备好 Down taskId["+task.getSellerId()+"] ["+task.getTaskId()+"] memo:"+task.getMemo());
								}
							} catch (Exception e) {
							}
						}
					//}
				}

			}
			Paginator paginator = pagelist.getPaginator();
			if (paginator.getNextPage() == paginator.getPage()) {
				break;
			}
			pageQuery.increasePageNum();
		}

		try {
			executorService.shutdown();
			executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}

class TaskDownFile implements Callable {

	private CouponTaskDo task;
	private Task taskNew;
	private CouponTaskDao couponTaskDao;
	private SellerDo sellerDo;
	public TaskDownFile(SellerDo sellerDo,CouponTaskDo task, Task taskNew,
			CouponTaskDao couponTaskDao) {
		this.task = task;
		this.taskNew = taskNew;
		this.couponTaskDao = couponTaskDao;
		this.sellerDo = sellerDo;
	}

	@Override
	public CouponTaskDo call() throws Exception {
		long start = System.currentTimeMillis();
		Long size=0l;
		try {
			CouponTaskDownFile.automic.incrementAndGet();

			String url = taskNew.getDownloadUrl();
			CouponTaskDownFile.logger.info("******* Downing file num:["+CouponTaskDownFile.automic.get()+"]sellerId:["+sellerDo.getId()+" / "+sellerDo.getNick()+"] taskId["+task.getId()+"]url:"+url);
			
			File taskFile = AtsUtils.download(url, new File(CouponTaskDownFile.zipFile)); // 下载文件到本地
			File resultFile = new File(CouponTaskDownFile.unZipFile,
					taskNew.getTaskId() + ""); // 解压后的结果文件夹
			File file = AtsUtils.ungzip(taskFile, resultFile); // 解压缩并写入到指定的文件夹

			task.setFilePath(file.getAbsolutePath());
			task.setMemo("fileOk");
			size=file.length();
		} catch (Exception ex) {
			task.setMemo("fail");
			task.setFilePath(ex.getMessage()); 
			CouponTaskDownFile.logger.info("******* Downing file Err sellerId:["+task.getSellerId()+"] taskId["+task.getId()+"] Err:"+ex.getMessage());
			//删除多余的
            if(StrUtils.isNotEmpty(ex.getMessage()) && ex.getMessage().indexOf("Duplicate entry")>-1&&ex.getMessage().indexOf("unid_seller_coupon")>-1){
                task.setMemo("duplicate");
            }
			ex.printStackTrace();
		} finally {
			CouponTaskDownFile.automic.decrementAndGet();
			CouponTaskDownFile.logger.info("******* Down file ok ]time:["+((System.currentTimeMillis()-start)/1000.00)+"] file length:["+size+"]");			
		}
		this.couponTaskDao.update(task);
		return task;
	}
}