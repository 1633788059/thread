package com.wangjubao.app.others.service.couponReport.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

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
//@Service("couponReportConsumeTask")
public class CouponReportConsumeTask implements CouponReportService
{

    static final Logger                               logger       = Logger.getLogger("couponconsumetask");

    private CouponTaskDao                             couponTaskDao;

    private ApiService                                apiService;

    private CouponSendLogDao                          couponSendLogDao;

    //存放一个线程map
    private static final Map<String, ExecutorService> EXECUTOR_MAP = new ConcurrentHashMap<String, ExecutorService>();

    private static final Map<String, Boolean>         FLAG_MAP     = new ConcurrentHashMap<String, Boolean>();

    public CouponReportConsumeTask()
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
                    //凌晨6点到10点执行
                    String tstart = "60000";
                    Integer endt = Integer.parseInt(tstart) + 230000;
                    String timenow = DateUtil.getTimeStr().replace(":", "");
                    boolean timeIsOk = false;
                    /*if (Integer.parseInt(timenow) >= Integer.parseInt(tstart) && Integer.parseInt(timenow) <= endt)
                    {
                        timeIsOk = true;
                    }

                    if (!timeIsOk)
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
                                            if (sellerDo != null)
                                            {
//                                                if(sellerDo.getId()==710024899l||sellerDo.getId()==1650679380l||sellerDo.getId()==202271589l||sellerDo.getId()==435878238l||sellerDo.getId()==741598534l
//                                                		||sellerDo.getId()==533230328l||sellerDo.getId()==1770004653l||sellerDo.getId()==720472756l||sellerDo.getId()==775871925l||sellerDo.getId()==811888884l
//                                                		||sellerDo.getId()==551001031l||sellerDo.getId()==667334702l || sellerDo.getId()==387947922l||sellerDo.getId()==1603022933l
//                                                		||sellerDo.getId()==1136802382l||sellerDo.getId()==2119549126l||sellerDo.getId()==321224345l || sellerDo.getId()==215312615l
//                                            			||sellerDo.getId()==27567810l||sellerDo.getId()==527299479l||sellerDo.getId()==720088591l||sellerDo.getId()==754742177l
//                                                		||sellerDo.getId()==1087950190l||sellerDo.getId()==444930503l||sellerDo.getId()==764088273l||sellerDo.getId()==901506476l||sellerDo.getId()==1677253488l
//                                                		||sellerDo.getId()==520430037l||sellerDo.getId()==812307535l){
                                                    doConsumeTask(sellerDo);
                                                    size = size - 1;
//                                                }
                                            }
                                            try
                                            {
//                                                logger.info(String.format(
//                                                        " 【%s】执行完成########################### 数据源%s,剩余【%s】个要执行",
//                                                        sellerDo.getNick(), key, size));
                                                sleep(2000);
                                            } catch (InterruptedException e)
                                            {
                                                e.printStackTrace();
                                            }
                                        }

                                    } catch (Exception e)
                                    {
                                        String error = StrUtils.showError(e);
                                        logger.error("消费报告错误1："+error);
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
                        sleep(1000 * 60 * 10);
                    } catch (InterruptedException e)
                    {
                        String error = StrUtils.showError(e);
                        logger.error("消费报告错误："+error);
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
        while (true)
        {
            PageList<CouponTaskDo> pagelist = this.couponTaskDao.listPageByTaskStatus(couponTaskDo, pageQuery);
            if (pagelist == null)
            {
                break;
            }
            for (CouponTaskDo task : pagelist)
            {
                //过滤掉已经失败的和成功的，且update时间在30分钟之前的
                if(!StrUtils.isNotEmpty(task.getMemo())||(!task.getMemo().equals("fail")&&!task.getMemo().equals("done")&&!task.getMemo().equals("empty")&&!task.getMemo().equals("duplicate")&&!task.getMemo().equals("notask"))){
                    if(task.getGmtModified().before(DateUtil.nextMinutes(-30))){
                        Task taskNew = this.apiService.getCouponreport(task.getSellerId(), task.getTaskId());
                        
                        if(taskNew!=null&&!"empty".equals(taskNew.getStatus())){
                            try
                            {
                                List<CouponSendLogDo> couponSendLoglist = new ArrayList<CouponSendLogDo>();
                                
                                String url = taskNew.getDownloadUrl();
                                logger.info(taskNew.getDownloadUrl()+">>>>>>>>>>>>>>>"+">>>>卖家："+task.getSellerId()+">>>>>>任务id"+task.getTaskId()+">>>>开始执行");
                                if(StrUtils.isNotEmpty(url)){
	                                long start = System.currentTimeMillis();
                                	 logger.info(taskNew.getDownloadUrl()+">>>>>>>>>>>>>>>"+">>>>卖家："+task.getSellerId()+">>>>>>任务id"+task.getTaskId()+">>>>开始下载文件");
	                                File taskFile = AtsUtils.download(url, new File("/home/deploy/couponTask/result/")); // 下载文件到本地
	                                File resultFile = new File("/home/deploy/couponTask/ungzip/", taskNew.getTaskId() + ""); // 解压后的结果文件夹
	                                File file = AtsUtils.ungzip(taskFile, resultFile); // 解压缩并写入到指定的文件夹
	                                logger.info(taskNew.getDownloadUrl()+">>>>>>>>>>>>>>>"+">>>>卖家："+task.getSellerId()+">>>>>>任务id"+task.getTaskId()+"》》路径"+file.getAbsolutePath()+"大小>>>>"+couponSendLoglist.size()+"》》》》下载并解压执行时间"+(System.currentTimeMillis() - start));
	                                
	                                // 读取结果文件进行解释 …
	                                start = System.currentTimeMillis();
	                                logger.info(taskNew.getDownloadUrl()+">>>>>>>>>>>>>>>"+">>>>卖家："+task.getSellerId()+">>>>>>任务id"+task.getTaskId()+">>>>开始读取文件");
		                               
	                                couponSendLoglist = readCsvForCouponTask(file,task);
	                                
	                                logger.info(taskNew.getDownloadUrl()+">>>>>>>>>>>>>>>"+">>>>卖家："+task.getSellerId()+">>>>>>任务id"+task.getTaskId()+"》》路径"+file.getAbsolutePath()+"大小>>>>"+couponSendLoglist.size()+"》》》》读取执行时间"+(System.currentTimeMillis() - start));
	                                boolean t = false;
	                                start = System.currentTimeMillis();
	                                logger.info(taskNew.getDownloadUrl()+">>>>>>>>>>>>>>>"+">>>>卖家："+task.getSellerId()+">>>>>>任务id"+task.getTaskId()+">>>>开始插入数据库");
		                             int excetue=0; 
	                                if(couponSendLoglist.size()>0){
										if (couponSendLoglist.size()<=200) {
											t = this.couponSendLogDao.createBatch(couponSendLoglist);
										}else{
											List<CouponSendLogDo> tempCouponSend = new ArrayList<CouponSendLogDo>();
											for (int i = 0; i < couponSendLoglist.size(); i++) {
												tempCouponSend.add(couponSendLoglist.get(i));
												excetue++;
												if (tempCouponSend.size()==200||i==tempCouponSend.size()-1) {
													 start = System.currentTimeMillis();
													t=this.couponSendLogDao.createBatch(couponSendLoglist);
													tempCouponSend.clear();
													if (!t) {
														break;
													}
													 logger.info(taskNew.getDownloadUrl()+">>>>>>>>>>>>>>>"+">>>>卖家："+task.getSellerId()+">>>>>>任务id"+task.getTaskId()+">>>>开始插入数据库200条耗时"+(System.currentTimeMillis() - start)+"当前执行："+excetue+"还剩下"+couponSendLoglist.size());
							                            
												}
												
											}
										}
	                                    if(t){
	                                        task.setMemo(taskNew.getStatus());
	                                        this.couponTaskDao.update(task);
	                                    }
	                                }
	                                logger.info(taskNew.getDownloadUrl()+">>>>>>>>>>>>>>>"+">>>>卖家："+task.getSellerId()+">>>>>>任务id"+task.getTaskId()+"》》》》插入数据执行时间"+(System.currentTimeMillis() - start));
	                                
                                }
                                
                            } catch (Exception e)
                            {
                                String error = StrUtils.showError(e);
                                logger.error(">>>>>>>解析文件错误"+">>>>卖家："+task.getSellerId()+">>>>>>任务id"+task.getTaskId()+"error::"+error);
                                //删除多余的
                                if(StrUtils.isNotEmpty(error)&&error.indexOf("Duplicate entry")>-1&&error.indexOf("unid_seller_coupon")>-1){
	                                task.setMemo("duplicate");
									this.couponTaskDao.update(task);
                                }
                            }
                        }
                        if(taskNew!=null&&"empty".equals(taskNew.getStatus())){
                            task.setMemo(taskNew.getStatus());
                            this.couponTaskDao.update(task);
                        }
                    }
                }
                
            }
            Paginator paginator = pagelist.getPaginator();
            if (paginator.getNextPage() == paginator.getPage())
            {
                break;
            }
            pageQuery.increasePageNum();
        }
        
    }

    // 200M
    private List<CouponSendLogDo> readCsvForCouponTask(File file,CouponTaskDo task)
    {
        CsvReader reader = null;
        List<CouponSendLogDo> result = new ArrayList<CouponSendLogDo>();
        try
        {
            reader = new CsvReader(file.getAbsolutePath(), ',',Charset.forName("UTF-8"));
            reader.readHeaders();
            while (reader.readRecord())
            {
                CouponSendLogDo line = new CouponSendLogDo();
                try
                {
                    String[] csvline = reader.getValues();
                    if (csvline == null)
                    {
                        continue;
                    } else if (csvline.length < 5)
                    {
                        logger.info(String
                                .format("\u4e0d\u5408\u6cd5\u6587\u4ef6\uff0c\u65e0\u6cd5\u5bfc\u5165\uff0c\u8bf7\u68c0\u67e5\u6587\u4ef6\u683c\u5f0f\u3002 【%s】",
                                        Arrays.toString(csvline)));
                        continue;
                    }
                    line.setCouponId(Long.parseLong(csvline[0]));
                    line.setBuyerNick(csvline[1]);
                    line.setActivityId(task.getActivityId());
                    line.setActivityType("124");
                    line.setCouponChannel(Integer.valueOf(csvline[2]+""));
                    line.setUseTatus(csvline[3]);
                    line.setCouponNumber(csvline[4]);
                    line.setSellerId(task.getSellerId());
                    
                    result.add(line);
                    // ...
                } catch (Exception e)
                {
                    e.printStackTrace();
                    logger.error(String.format("{%s【 %s】}(%s) read excel error . ", task.getId(), task.getSellerId(),
                             Arrays.toString(e.getStackTrace())));
                }
            }
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
            logger.error(String.format("{%s【 %s】}(%s) read excel error .  ", task.getTaskId(), task.getSellerId(),
                     Arrays.toString(e.getStackTrace())));
        } catch (IOException e)
        {
            e.printStackTrace();
            logger.error(String.format("{%s【 %s】}(%s) read excel error .  ", task.getId(), task.getSellerId(),
                     Arrays.toString(e.getStackTrace())));
        }
        return result;
    }
}
