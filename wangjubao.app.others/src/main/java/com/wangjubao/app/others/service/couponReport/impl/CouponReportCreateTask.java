package com.wangjubao.app.others.service.couponReport.impl;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.drools.core.util.StringUtils;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.taobao.api.domain.Task;
import com.wangjubao.app.others.api.service.ApiService;
import com.wangjubao.app.others.service.couponReport.CouponReportService;
import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dao.ActivityInfoDao;
import com.wangjubao.dolphin.biz.dao.CouponTaskDao;
import com.wangjubao.dolphin.biz.model.ActivityInfoDo;
import com.wangjubao.dolphin.biz.model.CouponTaskDo;
import com.wangjubao.dolphin.biz.model.JsonActivityDetail;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.common.lang.Paginator;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.date.DateUtils;
import com.wangjubao.framework.util.DateUtil;

/**
 * @author luorixin
 * @date 2014-5-23 下午1:39:43
 * @version V1.0
 * @ClassName: CouponReportCreateTask
 * @Description: 优惠券使用情况获取返回任务id
 */
//@Service("couponReportCreateTask")
public class CouponReportCreateTask implements CouponReportService
{

    static final Logger                               logger       = Logger.getLogger("couponcreatetask");

    private ActivityInfoDao                           activityInfoDao;

    private CouponTaskDao                             couponTaskDao;

    private ApiService                                apiService;
    //存放一个线程map
    private static final Map<String, ExecutorService> EXECUTOR_MAP = new ConcurrentHashMap<String, ExecutorService>();

    private static final Map<String, Boolean>         FLAG_MAP     = new ConcurrentHashMap<String, Boolean>();
    
    private static GsonBuilder  builder                         = new GsonBuilder();
    private static Gson         gson                            = builder.create();

    public CouponReportCreateTask()
    {
        activityInfoDao = (ActivityInfoDao) SynContext.getObject("dolphinActivityInfoDao");
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
                    Integer endt = Integer.parseInt(tstart) + 160000;
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
                        	logger.info("Create Task Sleeping.......");
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

                    logger.info(String.format(
                            "需要执行 数据源%s个,=======================",
                            list.size()));
                    
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
//                                            	if(!sellerDo.getId().equals(862613498l))
//                                            		continue;
                                            	
                                                doCreateTask(sellerDo);
                                                size = size - 1;
                                            }
                                            try
                                            {
                                                logger.info(String.format(
                                                        " 【%s】执行完成########################### 数据源%s,剩余【%s】个要执行",
                                                        sellerDo.getNick(), key, size));
                                                sleep(2000);
                                            } catch (InterruptedException e)
                                            {
                                                e.printStackTrace();
                                            }
                                        }

                                    } catch (Exception e)
                                    {
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
                        sleep(1000 * 60 * 60);
                    } catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }

                }
            }
        }.start();

    }

    /**
     * 按店铺获取各个优惠券的id并调用接口获取对应的taskid 对应失败的任务，需要重新创建
     * 
     * @throws ParseException
     */
    private void doCreateTask(SellerDo sellerDo) throws ParseException
    {
        if (sellerDo != null && sellerDo.getSourceId() != null)
        {
            long start = System.currentTimeMillis();
            logger.info(String.format("{%s【%s】} begin create_task, time : 【%s】\t", sellerDo.getId(),
                    sellerDo.getNick(), DateUtils.formatDate(DateUtils.now())));
            //先将失败的任务重新生成 
            reBornTask(sellerDo);

            //遍历activityinfo表获取优惠券id并按每14天生成一个taskid插入是数据库中
            createTask(sellerDo);

            logger.info(String.format("{%s【%s】} create_task finished（%s）-ms | time : 【%s】\t \n", sellerDo.getId(),
                    sellerDo.getNick(), (System.currentTimeMillis() - start), DateUtils.formatDate(DateUtils.now())));
        }
    }

    private void createTask(SellerDo sellerDo)
    {
        ActivityInfoDo actInfo = new ActivityInfoDo();
        actInfo.setSellerId(sellerDo.getId());
//        actInfo.setCurrentStatus(2);
        actInfo.setActivityType("124");
        PageQuery pageQuery = new PageQuery(0, 4000);
        while (true)
        {
        	Long start = System.currentTimeMillis();
        	logger.info("******* Create Task begin ******* ");
            PageList<ActivityInfoDo> pageList = activityInfoDao.listActivityInfoByPage(actInfo, null, null, pageQuery);
            if (pageList == null)
            {
                break;
            }
            for (ActivityInfoDo activityInfoDo : pageList)
            {
                try
                {
                    Date couponbegin = activityInfoDo.getBeginDate();
                    Date couponend = activityInfoDo.getEndDate();
                   
                    if (activityInfoDo.getCouponReportTime() == null)
                    {
                        activityInfoDo.setCouponReportTime(DateUtils.str2Date("2014-04-01 00:00:00"));
                    }
                    
                    if(couponend!=null&&couponend.before(activityInfoDo.getCouponReportTime())){
                        continue;
                    }
                    if(couponbegin!=null && couponbegin.before(DateUtils.str2Date("2014-04-01 00:00:00"))){
                    	continue;
                    }
                        
                    if (activityInfoDo.getCouponReportTime().before(DateUtils.str2Date("2014-04-01 00:00:00")))
                    {
                        //在此之前的意味着重算，则清空t_coupon_task表的所有数据 
                        this.couponTaskDao.deleteByActId(activityInfoDo.getActivityId(), activityInfoDo.getSellerId());
                    }
                } catch (ParseException e)
                {
                    logger.error(">>>>>>>>>>>>时间格式转换错误");
                    e.printStackTrace();
                }
                
                
                doTask(activityInfoDo);
            }

            Paginator paginator = pageList.getPaginator();
            if (paginator.getNextPage() == paginator.getPage())
            {
                break;
            }
            logger.info("******* Create Task end ["+pageQuery.getPageNum()+"/"+pageQuery.getPageSize()+"] time:["+((System.currentTimeMillis()-start)/1000.00)+"] ");
            pageQuery.increasePageNum();
        }

    }

    //每14天调一次接口（接口限制）
    private void doTask(ActivityInfoDo activityInfoDo)
    {
        Date now = DateUtils.getFirstDate(new Date());
        Date start = DateUtils.getFirstDate(activityInfoDo.getCouponReportTime());
        Date couponbegin = DateUtils.getFirstDate(activityInfoDo.getBeginDate());
        Date couponend = DateUtils.getLastDate(activityInfoDo.getEndDate());
        
        if(couponbegin!=null&&couponbegin.after(start)){
            start = couponbegin;
        }
        if(couponend!=null&&couponend.before(now)){
            now = couponend;
        }
        int diff = 14;
        Date end = DateUtils.nextDays(start, diff);
        List<CouponTaskDo> couponTasklist = new ArrayList<CouponTaskDo>();
        
        if (end.after(now))
        {
            end = now;
        }
        do
        {
            if (start.before(now))
            {
                if (StringUtils.isEmpty(activityInfoDo.getActivityDetailgsontStr()))
                {
                    break;  
                }
                JsonActivityDetail detail =  gson.fromJson(activityInfoDo.getActivityDetailgsontStr(), JsonActivityDetail.class);
                if(StringUtils.isEmpty(detail.getCouponId())){
                    break;
                }
                Task task = this.apiService.getCoupondetail(activityInfoDo.getSellerId(), Long.valueOf(detail.getCouponId()), null, start, end);
                if(task!=null){
                    
                    CouponTaskDo couponTask = new CouponTaskDo();
                    couponTask.setSellerId(activityInfoDo.getSellerId());
                    couponTask.setActivityId(activityInfoDo.getActivityId());
                    couponTask.setStartTime(start);
                    couponTask.setEndTime(end);
                    couponTask.setCouponId(Long.valueOf(detail.getCouponId()));
                    couponTask.setTaskId(task.getTaskId());
                    couponTask.setMemo(task.getStatus());
                    couponTasklist.add(couponTask);
                }
            } else
            {
                break;
            }
            start = end;
            end = DateUtils.nextDays(start, diff);
            if (end.after(now))
            {
                end = now;
            }
        } while (true);

        try
        {
            if (couponTasklist.size() > 0)
            {
                //批量插入任务表
                boolean t = this.couponTaskDao.createBatch(couponTasklist);
                //结束后更新最后掉接口时间
                if (t)
                {
                    activityInfoDo.setCouponReportTime(start);
                    this.activityInfoDao.update(activityInfoDo);
                }
            }
        } catch (Exception e)
        {
            logger.error(">>>>>>>>>>>>数据库插入错误" + "》》》》》》》卖家" + activityInfoDo.toString());
            e.printStackTrace();
        }
    }

    private void reBornTask(SellerDo sellerDo)
    {
        PageQuery pageQuery = new PageQuery(0, 1000);
        CouponTaskDo couponTaskDo = new CouponTaskDo();
        couponTaskDo.setSellerId(sellerDo.getId());
        couponTaskDo.setMemo("fail");
        while (true)
        {
            PageList<CouponTaskDo> pagelist = this.couponTaskDao.listPageByTaskStatus(couponTaskDo, pageQuery);
            if (pagelist == null)
            {
                break;
            }
            for (CouponTaskDo task : pagelist)
            {
                
                Task taskNew = this.apiService.getCoupondetail(task.getSellerId(), task.getCouponId(), null, task.getStartTime(), task.getEndTime());
                
                if(taskNew!=null){
                    task.setTaskId(taskNew.getTaskId());
                    task.setMemo(taskNew.getStatus());
                    this.couponTaskDao.update(task);
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
}
