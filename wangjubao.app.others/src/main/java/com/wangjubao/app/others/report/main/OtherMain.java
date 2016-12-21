package com.wangjubao.app.others.report.main;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.quartz.Scheduler;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.google.gson.Gson;
import com.mysql.jdbc.StringUtils;
import com.wangjubao.app.others.consume.ConsumeCalculate;
import com.wangjubao.app.others.sellerInfo.SellerDataServer;
import com.wangjubao.app.others.service.AddBlackEmailService;
import com.wangjubao.app.others.service.AnomalyLogisticsService;
import com.wangjubao.app.others.service.RemindMessage;
import com.wangjubao.core.job.listener.CommonJobListener;
import com.wangjubao.core.job.listener.CommonSchedulerListener;
import com.wangjubao.core.service.job.OtherJobServer;
import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dao.ActivityInfoDao;
import com.wangjubao.dolphin.biz.dao.SellerDao;
import com.wangjubao.dolphin.biz.model.ActivityInfoDo;
import com.wangjubao.dolphin.biz.model.JsonActivityDetail;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.biz.service.job.AppContext;

/**
 * @author ckex created 2013-6-25 - 下午1:31:03 OtherMain.java
 * @explain -日报发送 异常物流 main
 */
public class OtherMain {
    public static final Logger     logger = Logger.getLogger(OtherMain.class);

    public static SellerDataServer sellerDataServer;

    public static ConsumeCalculate consumeCalculate;

    public static void main(String[] args) throws Exception
    {

        long start = System.currentTimeMillis();
        System.out.println(" ################## OtherMain start ... ");
        logger.info(String.format(" other main start (%s)", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:sss").format(new Date())));
        AppContext.setContext(new String[] { "classpath*:/META-INF/spring/*.xml", "classpath*:/spring/*.xml" });
        SynContext.setContext(AppContext.getContext());

        //异常物流 219
        /*************************************************************************************/

/*      迁移至物流服务   
 *      AnomalyLogisticsService anomalyLogisticsService = (com.wangjubao.app.others.service.AnomalyLogisticsService) AppContext
                .getBean("anomalyLogisticsService");
        anomalyLogisticsService.execute();
 */
        /*************************************************************************************/

        //日报219
        logger.info("---------------------启动 日报线程");
        sellerDataServer = new SellerDataServer();
        sellerDataServer.doDataFetch();
        logger.info("---------------------启动 成功");
        /*************************************************************************************/

        //消费计算219
        logger.info("---------------------启动 消费计算主线程");
        consumeCalculate = new ConsumeCalculate();
        consumeCalculate.init();
        consumeCalculate.doDataFetch();
        logger.info("---------------------消费计算主线程启动 成功");
        /*************************************************************************************/

        //每日短信提醒219
        //        logger.info("---------------------启动 每日短信提醒线程");
        RemindMessage remindMessage = (RemindMessage) AppContext.getBean("remindMessage");
        remindMessage.execute();
        logger.info("---------------------每日短信提醒线程启动 成功");
        /*************************************************************************************/

        // 导入邮件黑名单 113
        //        logger.info("---------------------导入邮件黑名单...");
        //        final AddBlackEmailService addBlackEmailService = (AddBlackEmailService) AppContext.getBean("addBlackEmailService");
        //
        //        final CountDownLatch countDownLatch = new CountDownLatch(1);
        //        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
        //        scheduledThreadPoolExecutor.schedule(new Runnable()
        //        {
        //
        //            @Override
        //            public void run()
        //            {
        //                addBlackEmailService.importBlackMobiles();
        //                countDownLatch.countDown();
        //            }
        //        }, 1, TimeUnit.MILLISECONDS);
        //        countDownLatch.await();
        //        scheduledThreadPoolExecutor.shutdown();
        /******************************************************************/

        logger.info(String.format(" other main started times :  (%s) -ms ", (System.currentTimeMillis() - start)));

        synchronized (OtherMain.class)
        {
            while (true)
            {
                try
                {
                    OtherMain.class.wait();
                } catch (Throwable e)
                {
                }
            }
        }
    }

    public static void initOtherJob() throws Exception
    {
        System.out.println(" othre job service  . ");
        logger.info(" otherJobService ");
        OtherJobServer jobService = (OtherJobServer) SynContext.getObject("otherJobService");
        Scheduler schedule2 = jobService.buildSchedule();
        schedule2.getListenerManager().addJobListener((CommonJobListener) SynContext.getObject("commonJobListener"));
        schedule2.getListenerManager().addSchedulerListener((CommonSchedulerListener) SynContext.getObject("commonSchedulerListener"));

        schedule2.start();
    }

    private static void executeForBizDal()
    {
        ApplicationContext context = new ClassPathXmlApplicationContext("classpath*:META-INF/spring/*.xml");
        SellerDao sellerDao = context.getBean(SellerDao.class);
        ActivityInfoDao activityInfoDao = context.getBean(ActivityInfoDao.class);
        List<SellerDo> sellers = sellerDao.listAll();
        for (SellerDo sellerDo : sellers)
        {
            if (!StringUtils.isEmptyOrWhitespaceOnly(sellerDo.getDatasourceKey()) && sellerDo.getDatasourceKey().equals("008"))
            {
                long time = System.currentTimeMillis();
                int num = 0;
                PageQuery pageQuery = new PageQuery(0, 10000);
                List<ActivityInfoDo> activityInfos = activityInfoDao.listByPage(sellerDo.getId(), pageQuery);
                for (ActivityInfoDo activityInfoDo : activityInfos)
                {
                    long t = System.currentTimeMillis();
                    String jsonDetail = activityInfoDo.getActivityDetailgsontStr();
                    JsonActivityDetail jsonActivityDetail = JsonActivityDetail.activityDetailJsonStrToBean(jsonDetail);
                    if (jsonActivityDetail != null)
                    {
                        boolean isUpdate = Boolean.FALSE;
                        Long emailTemplateId = jsonActivityDetail.getEmailTemplateId();
                        if (emailTemplateId != null)
                        {
                            emailTemplateId += 1000000000;
                            jsonActivityDetail.setEmailTemplateId(emailTemplateId);
                            isUpdate = Boolean.TRUE;
                        }
                        Long smsTemplateId = jsonActivityDetail.getSmsTemplateId();
                        if (smsTemplateId != null)
                        {
                            smsTemplateId += 1000000000;
                            jsonActivityDetail.setSmsTemplateId(smsTemplateId);
                            isUpdate = Boolean.TRUE;
                        }
                        String groupId = jsonActivityDetail.getGroupId();
                        if (!StringUtils.isEmptyOrWhitespaceOnly(groupId))
                        {
                            StringBuilder sb = new StringBuilder();
                            for (String gid : groupId.split(","))
                            {
                                sb.append(",");
                                sb.append(String.valueOf(Long.parseLong(gid) + 1000000000));
                            }
                            jsonActivityDetail.setGroupId(sb.toString().substring(1));
                            isUpdate = Boolean.TRUE;
                        }
                        if (isUpdate)
                        {
                            ++num;
                            jsonDetail = new Gson().toJson(jsonActivityDetail);
                            activityInfoDo.setActivityDetailgsontStr(jsonDetail);
                            activityInfoDao.newUpdate(activityInfoDo);
                            logger.info(" update activity json : " + sellerDo.getNick());
                            logger.info(String.format("[%s-%s-%s] update activity json time : %s -s ", sellerDo.getNick(),
                                    sellerDo.getId(), activityInfoDo.getActivityId(), ((System.currentTimeMillis() - t) / 1000.00)));
                        }
                    }
                }
                logger.info(String.format("[%s-%s] num : %s time : %s -s", sellerDo.getNick(), sellerDo.getId(), num,
                        ((System.currentTimeMillis() - time) / 1000.00)));
            }
        }
    }
}
