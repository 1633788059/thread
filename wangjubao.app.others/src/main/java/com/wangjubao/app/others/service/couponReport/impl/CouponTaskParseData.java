package com.wangjubao.app.others.service.couponReport.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

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
//@Service("CouponTaskParseData")
public class CouponTaskParseData implements CouponReportService
{

    static final Logger                               logger       = Logger.getLogger("couponconsumetask");

    private CouponTaskDao                             couponTaskDao;

    private ApiService                                apiService;

    private CouponSendLogDao                          couponSendLogDao;

    //存放一个线程map
    private static final Map<String, ExecutorService> EXECUTOR_MAP = new ConcurrentHashMap<String, ExecutorService>();

    private static final Map<String, Boolean>         FLAG_MAP     = new ConcurrentHashMap<String, Boolean>();
    public static AtomicInteger automic = new AtomicInteger(0);
    
    public CouponTaskParseData()
    {
        couponSendLogDao = (CouponSendLogDao) SynContext.getObject("couponSendLogDao");
        couponTaskDao = (CouponTaskDao) SynContext.getObject("couponTaskDao");
        apiService = (ApiService) SynContext.getObject("apiService");
    }

    @Override
    public void doWork()
    {
        new Thread()
        {
            public void run()
            {
                while (true)
                {
                	logger.info("******* Parse file running *********");
                    //凌晨6点到10点执行
                    String tstart = "00000";
                    Integer endt = Integer.parseInt(tstart) + 160000;
                    String timenow = DateUtil.getTimeStr().replace(":", "");
                    boolean timeIsOk = false;
                    if (Integer.parseInt(timenow) >= Integer.parseInt(tstart) && Integer.parseInt(timenow) <= endt)
                    {
                        timeIsOk = true;
                    }

                    /*if (false)
                    {
                        try
                        {
                            sleep(1000 * 60 * 30);
                        } catch (InterruptedException e)
                        {
                            e.printStackTrace();
                        }

                        continue;
                    }*/
                    //获取所有卖家
                    Map<String, List<SellerDo>> map = CouponSellerMap.sellerMap;

                    Set<Entry<String, List<SellerDo>>> list = map.entrySet();

                    for (Iterator<Entry<String, List<SellerDo>>> it = list.iterator(); it.hasNext();)
                    {
                        Entry<String, List<SellerDo>> entry = it.next();

                        /*
                         * ①每个数据源一个线程（线程池）
                         */

                        //②根据flag来判定该卖家是否属于同个数据源，同个的话该线程就wait，不同的话就执行
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
                                            final SellerDo sellerDo = sellerit.next(); 
                                            if(sellerDo.getId().intValue()%1000==0)
            									logger.info("Down>>.....");
                                            if(CouponTaskParseData.check(sellerDo)){
                                                logger.info("Parse>>"+sellerDo.getId()+":"+sellerDo.getNick());
                                                doConsumeTask(sellerDo);
                                                size = size - 1;
                                            } 
                                        }

                                    } catch (Exception e)
                                    {
                                        String error = StrUtils.showError(e);
                                        logger.error("## Parse Data Error 1："+error);
                                        e.printStackTrace();
                                    } finally
                                    {
                                        FLAG_MAP.put(key, Boolean.TRUE);
                                    }
                                }

                            });
                        }
                    }

                    try
                    {
                    	logger.info("Parse Task Sleeping......2.............................");
                        sleep(1000 * 60 * 30);
                    } catch (InterruptedException e)
                    {
                        String error = StrUtils.showError(e);
                        logger.error("## Parse Data Error 2："+error);
                        e.printStackTrace();
                    }

                }
            }
        }.start();

    }
   
    
    private void doConsumeTask(SellerDo sellerDo)
    {
        PageQuery pageQuery = new PageQuery(0, 1000);
        CouponTaskDo couponTaskDo = new CouponTaskDo();
        couponTaskDo.setSellerId(sellerDo.getId());
        couponTaskDo.setMemo("fileOk");
        while (true)
        {
            PageList<CouponTaskDo> pagelist = this.couponTaskDao.listPageByTaskStatus(couponTaskDo, pageQuery);
            if (pagelist == null)
            {
                break;
            }
            for (CouponTaskDo task : pagelist)
            {
                try
                {	
                	logger.info("**** Parse taskId ["+task.getTaskId()+"] memo:"+task.getMemo());
                	this.parse(sellerDo,task);
                	task.setMemo("done");
                } catch (Exception e)
                { 
                	task.setMemo("fail");
                	e.printStackTrace();
                }
                this.couponTaskDao.update(task);
            
            }
            Paginator paginator = pagelist.getPaginator();
            if (paginator.getNextPage() == paginator.getPage())
            {
                break;
            }
            pageQuery.increasePageNum();
        }
        
    }
    
    public void parse(SellerDo sellerDo,CouponTaskDo taskDo) throws Exception {
    	logger.info("*******Begin Parseing *********");
		BufferedReader reader = null;
		String tmp = null;
		int count = 0;
		List<CouponSendLogDo> couponSendLoglist = new ArrayList<CouponSendLogDo>();
		ExecutorService executorService = Executors.newFixedThreadPool(10);
		Future future;
		List<Future> futureList=new ArrayList<Future>();
		try {
			Long start=System.currentTimeMillis();
			Integer isParty = couponSendLogDao.findOne(taskDo.getTaskId(), sellerDo.getId());
			if(isParty.intValue()>0){
				int dnum = couponSendLogDao.deleteByTask(taskDo.getTaskId(), sellerDo.getId());
				logger.info("*******Seller:["+sellerDo.getId()+" / "+sellerDo.getNick()+"] delete: [ "+dnum+" ] time:"+((System.currentTimeMillis()-start)/1000.00));
			}
			
			File file = new File(taskDo.getFilePath());
			reader = new BufferedReader(new FileReader(file));
			CouponSendLogDo sendDo = null;
			if ((tmp = reader.readLine()) != null) {
				//第一行略... 
			}
			while ((tmp = reader.readLine()) != null) { 
				sendDo = logDo(tmp);
				if (null != sendDo) {
					count++;
					sendDo.setId(couponSendLogDao.getSequence());
					sendDo.setActivityId(taskDo.getActivityId());
					sendDo.setSellerId(taskDo.getSellerId());
					sendDo.setTaskId(taskDo.getTaskId());
					couponSendLoglist.add(sendDo);
				}
				if (count % 500 == 0) {
					logger.info("*******Seller:["+sellerDo.getId()+" / "+sellerDo.getNick()+"] Save Data["+count+"] *********");
					future = executorService.submit(new TaskParseData(sellerDo, couponSendLoglist, couponSendLogDao));
					couponSendLoglist=new ArrayList<CouponSendLogDo>();
					futureList.add(future);
				}
			}
			future = executorService.submit(new TaskParseData(sellerDo, couponSendLoglist, couponSendLogDao));
			futureList.add(future);
			taskDo.setMemo("done");
			try{
				file.deleteOnExit();
				logger.info("#####Seller:["+sellerDo.getId()+" / "+sellerDo.getNick()+"] file deleted *********");
			}catch(Exception ex){
				
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			taskDo.setMemo("fail");
		} finally {
			couponSendLoglist = null;
			if (null != reader)
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			
			executorService.shutdown();
			executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		}
		this.couponTaskDao.update(taskDo);
		logger.info("******************* all thread complete *******************");
	}

	public CouponSendLogDo logDo(String lineStr) {
		if (StringUtils.isEmpty(lineStr))
			return null;
		CouponSendLogDo line = new CouponSendLogDo();
		String[] csvline = lineStr.split(",");
		if (csvline.length < 5)
			return null;
		line.setCouponId(Long.parseLong(csvline[0]));
		line.setBuyerNick(csvline[1]);
		line.setActivityType("124");
		line.setCouponChannel(Integer.valueOf(csvline[2] + ""));
		line.setUseTatus(csvline[3]);
		line.setCouponNumber(csvline[4]);
		return line;
	}
	 
    public static boolean check(SellerDo sellerDo){
//    	if(sellerDo==null)
//    		return false;
//    	Long sellerId=sellerDo.getId();
//    	if(sellerId.equals(710024899l)||sellerId.equals(1650679380l)||sellerId.equals(202271589l)||sellerId.equals(435878238l)||sellerId.equals(741598534l
//        		)||sellerId.equals(533230328l)||sellerId.equals(1770004653l)||sellerId.equals(720472756l)||sellerId.equals(775871925l)||sellerId.equals(811888884l
//        		)||sellerId.equals(551001031l)||sellerId.equals(667334702l )|| sellerId.equals(387947922l)||sellerId.equals(1603022933l
//        		)||sellerId.equals(1136802382l)||sellerId.equals(2119549126l)||sellerId.equals(321224345l )|| sellerId.equals(215312615l
//    			)||sellerId.equals(27567810l)||sellerId.equals(527299479l)||sellerId.equals(720088591l)||sellerId.equals(754742177l
//        		)||sellerId.equals(1087950190l)||sellerId.equals(444930503l)||sellerId.equals(764088273l)||sellerId.equals(901506476l)||sellerId.equals(1677253488l
//        		)||sellerId.equals(520430037l)||sellerId.equals(812307535l))
//    		return true;
    	return true;
    }
 
}

class TaskParseData implements Callable {

	private List couponSendLoglist;
	private CouponSendLogDao couponSendLogDao;
	private SellerDo sellerDo;

	public TaskParseData(SellerDo sellerDo,List couponSendLoglist,
			CouponSendLogDao couponSendLogDao) {
		this.couponSendLoglist = couponSendLoglist;
		this.couponSendLogDao = couponSendLogDao;
		this.sellerDo = sellerDo;
	}

	@Override
	public String call() throws Exception {
		CouponTaskParseData.automic.incrementAndGet();
		Long start=System.currentTimeMillis();
		couponSendLogDao.createBatch(couponSendLoglist);
		CouponTaskParseData.logger.info("*******["+sellerDo.getId()+" / "+sellerDo.getNick()+"]:["+CouponTaskParseData.automic.get()+"] Saveing size:["+couponSendLoglist.size()+"] ThreadId:["+ Thread.currentThread().getId()+"]time:["+((System.currentTimeMillis()-start)/1000.00)+"]*");
		CouponTaskParseData.automic.decrementAndGet(); 
		return "ok";
	}
}
