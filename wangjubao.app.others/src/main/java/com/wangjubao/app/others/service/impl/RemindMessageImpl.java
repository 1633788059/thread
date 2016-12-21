/**
 * 
 */
package com.wangjubao.app.others.service.impl;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.wangjubao.app.others.service.RemindMessage;
import com.wangjubao.core.domain.marketing.ActivityInfo;
import com.wangjubao.core.domain.sms.SmsAccount;
import com.wangjubao.core.domain.sms.SmsBean;
import com.wangjubao.core.domain.sms.SmsLog;
import com.wangjubao.core.service.basic.IUserService;
import com.wangjubao.core.service.syn.IFacadeService;
import com.wangjubao.dolphin.biz.common.constant.AdsStateEnum;
import com.wangjubao.dolphin.biz.common.constant.ProductType;
import com.wangjubao.dolphin.biz.common.constant.SellerType;
import com.wangjubao.dolphin.biz.dal.extend.bo.SellerBo;
import com.wangjubao.dolphin.biz.dao.ConsumeLogDao;
import com.wangjubao.dolphin.biz.dao.RemindDetailDao;
import com.wangjubao.dolphin.biz.dao.SellerDao;
import com.wangjubao.dolphin.biz.model.RemindDetailDo;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.biz.model.extend.AppParameterTaobao;
import com.wangjubao.dolphin.biz.service.SellerSessionKeyService;
import com.wangjubao.dolphin.common.util.StrUtils;
import com.wangjubao.dolphin.common.util.date.DateUtils;
import com.wangjubao.tool.email.service.EmailSendService;
import com.wangjubao.tool.email.service.impl.EmailSendServiceImpl;

/**
 * @author ckex created 2013-9-17 - 上午10:19:20 RemindMessageImpl.java
 * @explain -
 */
@Service("remindMessage")
public class RemindMessageImpl implements RemindMessage {

    private transient final static Logger           logger                      = Logger.getLogger("remindMessage");

    private Gson                                    gson                        = new Gson();
    @Autowired
    private SellerDao                               sellerDao;

    @Autowired
    @Qualifier("userService")
    private IUserService                            userService;

    @Autowired
    @Qualifier("facadeService")
    private IFacadeService                          facadeService;

    @Autowired
    @Qualifier("consumeLogDao")
    private ConsumeLogDao                           consumeLogDao;

    @Autowired
    @Qualifier("sellerSessionKeyService")
    private SellerSessionKeyService                 sellerSessionKeyService;

    @Autowired
    private RemindDetailDao                         remindDetailDao;

    @Autowired
    private SellerBo                                sellerBo;

    private EmailSendService                        emailSendService            = new EmailSendServiceImpl();

    private String[]                                emails                      = new String[] { "longmao@wangjubao.com" };

    public static final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(5);

    private static long                             INTERVAL                    = 60 * 60 * 24;

    @Override
    public void execute()
    {
        logger.info(" ################################## start RemindMessage");
        boolean flag = Boolean.FALSE;
        long initialDelay = 10;

        int hour = DateUtils.nowHour();
        if (hour < 17)
        { // 17点前,则等到17:10点半执行
            Calendar c = DateUtils.getCalendar();
            c.setTime(DateUtils.now());
            c.set(Calendar.HOUR_OF_DAY, 17);
            c.set(Calendar.MINUTE, 10);
            initialDelay = (c.getTime().getTime() - DateUtils.now().getTime()) / 1000;
        } else if (hour >= 10)
        {// 17点后,则等到下一天执行
            Calendar c = DateUtils.getCalendar();
            c.setTime(DateUtils.nextDays(DateUtils.now(), 1));
            c.set(Calendar.HOUR_OF_DAY, 17);
            c.set(Calendar.MINUTE, 10);
            initialDelay = (c.getTime().getTime() - DateUtils.now().getTime()) / 1000;
            flag = true;
        }

        if (flag)
        {
            start();
        }
        scheduledThreadPoolExecutor.scheduleWithFixedDelay(new Runnable()
        {

            @Override
            public void run()
            {
                logger.info("#############################start::" + DateUtils.formatDate(DateUtils.now()));
                start();
            }

        }, initialDelay, INTERVAL, TimeUnit.SECONDS);

    }

    private void start()
    {
        scheduledThreadPoolExecutor.schedule(new Runnable()
        {

            @Override
            public void run()
            {
                send();
            }

        }, 10, TimeUnit.MILLISECONDS);

    }

    private void send()
    {
        List<SellerDo> list = sellerDao.listActiveCompanySeller();
        if (list == null || list.isEmpty())
        {
            throw new IllegalArgumentException(" sellers is empty . ");
        }

        Map<String, Date> map = new HashMap<String, Date>();
        for (SellerDo sellerDo : list)
        {
            if (sellerDo.getId() != 331869718l)
            {
                //                continue; //测试
            }
            int week = DateUtils.dayOfWeek();
            /*******************************************************************/
            try
            {
                if (week == 1
                        && (sellerDo.getSellerType().intValue() == SellerType.TAOBAO || sellerDo.getSellerType().intValue() == SellerType.TMALL))
                {
                    AppParameterTaobao appParameterTaobao = sellerSessionKeyService.getSellerParameterById(sellerDo.getId(),
                            ProductType.CRM.getValue());
                    if (appParameterTaobao != null)
                    {
                        String dateStr = appParameterTaobao.getExpiresIn();
                        if (StringUtils.isNotBlank(dateStr))
                        {
                            Date date = new Date(Long.parseLong(dateStr));
                            if (logger.isInfoEnabled())
                            {
                                logger.info(String.format("[%s-%s]", sellerDo.getNick(), DateUtils.formatDate(date)));
                            }
                            if (date.before(DateUtils.nextDays(DateUtils.now(), 30)))
                            {
                                map.put(sellerDo.getId().toString(), date);
                                //                                sendSessionInvalidMail(sellerDo, date);
                            }
                        }
                    }
                }
            } catch (Exception e)
            {
                e.printStackTrace();
                logger.error(StrUtils.showError(e));
            }
            /*******************************************************************/

            Map<String, String> details = new HashMap<String, String>();
            Date during = DateUtils.getNextDays(DateUtils.now(), -1);
            RemindDetailDo remindDetail = new RemindDetailDo();
            remindDetail.setSellerId(sellerDo.getId());
            details.put("during", DateUtils.formatDate(during));
            remindDetail.setDescr(gson.toJson(details));
            remindDetail.setState(AdsStateEnum.NEW.getState());
            remindDetail = remindDetailDao.create(remindDetail);

            logger.info(String.format("创建明细表:%s", remindDetail.toString()));

            Integer sumUsage = consumeLogDao.sumSmsUsage(sellerDo.getId(), during);
            if (sumUsage == null || sumUsage.intValue() == 0)
            {
                String info = String.format(" #### {%s【%s】}(%s)没有消费短信息.不做提醒 . ", sellerDo.getId(), sellerDo.getNick(),
                        DateUtils.formatDate(during));
                details.put("info", info);
                details.put("failed",
                        " 没有消费短信或消费记录没有计算完成 [consumeLogDao.sumSmsUsage(" + sellerDo.getId() + "," + DateUtils.formatDate(during)
                                + ") is null or 0 ]");
                remindDetail.setDescr(gson.toJson(details));
                remindDetail.setState(AdsStateEnum.FAILED.getState());
                remindDetailDao.update(remindDetail);
                logger.warn(info);
                continue;
            }

            SmsAccount smsAccount = new SmsAccount();
            smsAccount.setSellerId(sellerDo.getId());
            List<SmsAccount> ss = userService.querySmsAccountList(smsAccount);
            Long remain = null;
            if (ss != null && !ss.isEmpty())
            {
                // 取第一条，与首页上方保持一致
                smsAccount = ss.get(0);
                if (smsAccount != null)
                {
                    remain = smsAccount.getRemain() == null ? 0 : smsAccount.getRemain().longValue();
                }
            }

            if (remain != null)
            {
                details.put("AcceptedSmsUsage", (sellerDo.getAcceptedSmsUsage() ? "Y" : "N"));
                details.put("AcceptedSmsBalanceLimit", (sellerDo.getAcceptedSmsBalanceLimit() ? "Y" : "N"));
                details.put("Remain", String.valueOf(remain));
                details.put("SmsBalanceLimit",
                        String.valueOf((sellerDo.getSmsBalanceLimit() == null ? 0 : sellerDo.getSmsBalanceLimit())));
                //用量提醒
                if (sellerDo.getAcceptedSmsUsage())
                {
                    smsSend(sellerDo, remain, ActivityInfo.ACTIVITY_SMS_USAGE, sumUsage, details);
                }
                //临界值提醒
                if (sellerDo.getAcceptedSmsBalanceLimit())
                {
                    if (remain <= sellerDo.getSmsBalanceLimit())
                    {
                        smsSend(sellerDo, remain, ActivityInfo.ACTIVITY_REMIND, null, details);
                    }
                }
                remindDetail.setState(AdsStateEnum.SUCCEEDED.getState());
                remindDetail.setDescr(gson.toJson(details));
                remindDetailDao.update(remindDetail);
            } else
            {
                details.put("remain", "NULL");
                remindDetail.setDescr(gson.toJson(details));
                remindDetail.setState(AdsStateEnum.FAILED.getState());
                remindDetailDao.update(remindDetail);
            }
        }
        sendSessionInvalidMail(map);
    }

    private void sendSessionInvalidMail(Map<String, Date> map)
    {
        try
        {
            String subject, html;
            subject = "[重要]网聚宝店铺过期提醒邮件";
            StringBuffer sb = new StringBuffer();
            if (map != null && !map.isEmpty())
            {
                for (Iterator<Entry<String, Date>> iterator = map.entrySet().iterator(); iterator.hasNext();)
                {
                    Entry<String, Date> entry = iterator.next();
                    Long sellerId = Long.valueOf(entry.getKey());
                    Date value = entry.getValue();
                    SellerDo sellerDo = sellerBo.findBySellerId(sellerId);
                    if (sellerDo != null)
                    {
                        sb.append("\t店铺名:[").append(sellerDo.getNick()).append("]");
                        sb.append("订购过期时间-[").append(DateUtils.formatDate(value)).append("]");
                        sb.append("\t\r\n");
                    }
                }
                html = "<html xmlns='http://www.w3.org/1999/xhtml'> " + "<head> "
                        + "<title>网聚宝提醒信息</title> <style type='text/css'>  body { background-color:red;  }  </style> " + "</head>"
                        + "<body>" + sb.toString() + "</body>" + "</html>";
                emailSendService.emailSend(subject, emails, html);
            }
            logger.info(String.format("[%s]发送提醒邮件.", DateUtils.formatDate(DateUtils.now())));
        } catch (Exception e)
        {
            e.printStackTrace();
            String info = StrUtils.showError(e);
            System.out.println(info);
            logger.error(info);
        }
    }

    /**
     * 要做记录，哪些卖家已经发过，哪些还没有发过
     * 
     * @param sellerDo
     * @param remain 短信的剩余量
     * @param activityType 提醒类型
     * @param usageSum 当天的短信使用量
     * @param details
     */
    private void smsSend(SellerDo sellerDo, Long remain, String activityType, Integer usageSum, Map<String, String> details)
    {
        String mobiles = sellerDo.getNotifyMobiles();
        details.put("mobiles", mobiles);
        if (StringUtils.isNotBlank(mobiles))
        {
            String[] arrMobile = mobiles.split(";");
            if (arrMobile != null && arrMobile.length > 0)
            {
                for (int i = 0; i < arrMobile.length; i++)
                {
                    if (StrUtils.isMobile(arrMobile[i]))
                    {

                        List<SmsLog> logs = userService.listSmsLog(sellerDo.getId(), arrMobile[i], activityType, DateUtils.now());

                        if (logs == null || logs.isEmpty())
                        { // 当天没有发送过提醒短信

                            SmsLog smsLog = new SmsLog();
                            smsLog.setSellerId(sellerDo.getId());
                            smsLog.setMobiles(arrMobile[i]);
                            smsLog.setEventType(activityType);
                            smsLog.setStatus(SmsBean.STATUS_WAITING);
                            smsLog.setCreateTime(DateUtils.now());
                            String content;
                            if (StringUtils.equalsIgnoreCase(ActivityInfo.ACTIVITY_REMIND, activityType))
                            {

                                content = "尊敬的客户：截止目前,\"" + sellerDo.getNick() + "\"店铺的短信余额已不足" + remain + "条,请及时充值,以免影响您的使用。【网聚宝】";

                                smsLog.setSmsContent(content);
                                facadeService.sendSms(smsLog);

                            } else if (StringUtils.equalsIgnoreCase(ActivityInfo.ACTIVITY_SMS_USAGE, activityType))
                            {

                                content = "您的店铺\"" + sellerDo.getNick() + "\"昨天消耗了" + usageSum + "条短信,剩余" + remain
                                        + "条,有疑问请联系QQ:2880167868【网聚宝】";
                                smsLog.setSmsContent(content);
                                facadeService.sendSms(smsLog);
                            }

                            String sentinfo = String.format("{%s【%s】} %s,使用 (%s)条, 余(%s)条, send finished . ", sellerDo.getId(),
                                    sellerDo.getNick(), smsLog.getMobiles(), usageSum, remain);
                            details.put("logid", (smsLog.getLogId() == null ? "NULL" : smsLog.getLogId().toString()));
                            details.put("sentinfo", sentinfo);
                            if (logger.isInfoEnabled())
                            {
                                logger.info(sentinfo);
                            }
                        } else
                        {
                            details.put("mobile", arrMobile[i] + "@重复发送");
                        }
                    }
                }
            }
        }
    }

    // 52   尊敬的客户：截止目前,"南汇鞋庄"店铺的短信余额已不足100条,请及时充值,以免影响您的使用。【网聚宝】
    public static void main(String[] args)
    {
        int remain = 100;
        String nick = "南汇鞋庄";
        String content = "【网聚宝】尊敬的客户：截止目前,\"" + nick + "\"店铺的短信余额已不足" + remain + "条,请及时充值,以免影响您的使用。";
        System.out.println(content.length() + "   " + content);
    }
}
