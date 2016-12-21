package com.wangjubao.app.others.service.consume.impl;

import static com.wangjubao.dolphin.common.util.MathsUtils.add;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.app.others.consume.ConsumeCalculate;
import com.wangjubao.app.others.service.consume.ConsumeCalculateService;
import com.wangjubao.core.dao.IActivityInfoDao;
import com.wangjubao.core.dao.IUserDao;
import com.wangjubao.core.domain.marketing.ActivityInfo;
import com.wangjubao.core.domain.sms.SmsLog;
import com.wangjubao.dolphin.biz.common.constant.ActivityInfoType;
import com.wangjubao.dolphin.biz.common.constant.BaseType;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dao.CompanyDao;
import com.wangjubao.dolphin.biz.dao.ConsumeLogDao;
import com.wangjubao.dolphin.biz.dao.EmailSentDao;
import com.wangjubao.dolphin.biz.dao.SellerDao;
import com.wangjubao.dolphin.biz.dao.SmsLogDao;
import com.wangjubao.dolphin.biz.model.BuyerDayDo;
import com.wangjubao.dolphin.biz.model.CompanyDo;
import com.wangjubao.dolphin.biz.model.CompanySellerDo;
import com.wangjubao.dolphin.biz.model.ConsumeLogDo;
import com.wangjubao.dolphin.biz.model.EmailSentDo;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.biz.model.SmsLogDo;
import com.wangjubao.dolphin.common.lang.Paginator;
import com.wangjubao.dolphin.common.util.Day;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.date.DateUtils;
import com.wangjubao.framework.page.DataList;

@Service("consumeCalculateService")
public class ConsumeCalculateServiceImpl implements ConsumeCalculateService {

    private static SimpleDateFormat sdf    = new SimpleDateFormat("yyyy-MM-dd");

    private static Logger           logger = LoggerFactory.getLogger("consume");

    @Autowired
    private ConsumeLogDao           consumeLogDao;
    @Autowired
    private EmailSentDao            emailSentDao;
    @Autowired
    private IUserDao                userDao;
    @Autowired
    private CompanyDao              companyDao;
    @Autowired
    private IActivityInfoDao        activityInfoDao;
    @Autowired
    private SmsLogDao               smsLogDao;

    /**
     * 开始计算每日消费信息
     * 
     * @throws ParseException
     */
    @Override
    public void calculateConsumeForDate(Long sellerId, Date startDate) throws ParseException {
        // TODO Auto-generated method stub
       /* //获取最近一次计算成功记录时间
        Date minConsumeTime = consumeLogDao.loadMinConsumeTime(sellerId);
        //select min(createTime) from t_sms_log where sellerId = #sellerId:BIGINT#
        Date minSmsLogTime = consumeLogDao.loadMinSmsLogTime(sellerId);
        Date minEmailLogTime = consumeLogDao.loadMinEmailLogTime(sellerId);
        //获取短信，邮件日志历史时间
        //select min(createTime) from t_sms_log_history where sellerId = #sellerId:BIGINT#
        //select min(createTime) from t_email_sent where sellerId = #sellerId:BIGINT#
        Date minSmsLogHistoryTime = consumeLogDao.loadMinSmsLogHistoryTime(sellerId);
        Date minEmailLogHistoryTime = consumeLogDao.loadMinEmailLogHistoryTime(sellerId);
        
        if (minSmsLogTime == null && minEmailLogTime == null && minSmsLogHistoryTime==null && minEmailLogHistoryTime==null) {
            logger.info(String.format("{ %s } the seller consume is null . ", sellerId));
            return;
        }
        if(minSmsLogHistoryTime!=null){
            if (minSmsLogTime==null||minSmsLogHistoryTime.before(minSmsLogTime)) {
                minSmsLogTime=minSmsLogHistoryTime;
            } 
        }
        
        if(minEmailLogHistoryTime!=null){
            if (minEmailLogTime==null||minEmailLogHistoryTime.before(minEmailLogTime)) {
                minEmailLogTime = minEmailLogHistoryTime;
            } 
        }
        
        if (minConsumeTime == null) {
            if (minSmsLogTime != null) {
                if (minEmailLogTime==null||minSmsLogTime.before(minEmailLogTime)) {
                    minConsumeTime = DateUtils.str2Date(
                            DateUtils.formatDate(minSmsLogTime, "yyyy-MM-dd"), sdf);
                } else {
                    minConsumeTime = DateUtils.str2Date(
                            DateUtils.formatDate(minEmailLogTime, "yyyy-MM-dd"), sdf);
                }
            } else if (minEmailLogTime != null) {
                minConsumeTime = DateUtils.str2Date(
                        DateUtils.formatDate(minEmailLogTime, "yyyy-MM-dd"), sdf);
                
            }

        }else{
            Date minErrorConsumeTime = consumeLogDao.loadMinErrorConsumeTime(sellerId);
            if(minErrorConsumeTime!=null){
                if(minErrorConsumeTime.before(minConsumeTime)){
                    minConsumeTime=minErrorConsumeTime;
                }
            }
        }
        if (startDate == null || startDate.before(minConsumeTime)) {
            startDate = minConsumeTime;//DateUtils.nextDays(minConsumeTime, -1);
        }*/

    	//取最近一次计算时间
    	Date lastConsumeTime = consumeLogDao.loadMinConsumeTime(sellerId);
    	logger.info("最近一次计算时间:"+lastConsumeTime);
    	 Date earliestDate = null;
        //判断时间是否null
    	if(lastConsumeTime == null){
    		//null: 取minSMS 先取历史，如果历史不为空取历史的，否则取现在的
    		//最早短信时间
    		 Date earliestSmsLogTime = null;
    		 //最早历史短信时间
    		 Date earliestSmsLogHistoryTime = consumeLogDao.loadMinSmsLogHistoryTime(sellerId);
    		 logger.info("最早[历史]短信时间:"+earliestSmsLogHistoryTime);
    		 if(earliestSmsLogHistoryTime == null){
    			  earliestSmsLogTime = consumeLogDao.loadMinSmsLogTime(sellerId);
    			  logger.info("最早短信时间:"+earliestSmsLogTime);
    		 }else{
    			 earliestSmsLogTime = earliestSmsLogHistoryTime;
    		 }
    		//最早邮件时间
    		 Date earliestEmailLogTime = null;
    		 //最早历史邮件时间
    		 Date earliestEmailLogHistoryTime = consumeLogDao.loadMinEmailLogHistoryTime(sellerId);
    		 logger.info("最早[历史]邮件时间:"+earliestEmailLogHistoryTime);
    		 //取minEmail 先取历史，如果历史不为空取历史的，否则取现在的
    		 if(earliestEmailLogHistoryTime == null){
    			 earliestEmailLogTime = consumeLogDao.loadMinEmailLogTime(sellerId);
    			 logger.info("最早邮件时间:"+earliestEmailLogTime);
    		 }else{
    			 earliestEmailLogTime = earliestEmailLogHistoryTime;
    		 }
    		 // 两者取最小值
    		 if(earliestSmsLogTime != null && earliestEmailLogTime == null){//短信时间不为空，邮件时间为空，取短信时间
    			 earliestDate=earliestSmsLogTime;
    		 }else if(earliestSmsLogTime == null && earliestEmailLogTime != null){//短信时间为空，邮件时间不为空，取邮件时间
    			 earliestDate=earliestEmailLogTime;
    		 }else if(earliestSmsLogTime != null && earliestEmailLogTime != null){//两个时间都不为空，取最小时间
    			 earliestDate = earliestSmsLogTime.before(earliestEmailLogTime)?earliestSmsLogTime:earliestEmailLogTime;
    		 }else{//两者都为空
    			 logger.info(String.format("{ %s } the last time of sms or email is null . ", sellerId));
    			 return;
    		 }
    	}else{
    		//not null:取最近一次错误时间 判断是否在最近一次成功之前 如果之前 用最近一次错误的时间
    		 Date earliestErrorConsumeTime = consumeLogDao.loadMinErrorConsumeTime(sellerId);
    		 logger.info("最近一次错误时间:"+earliestErrorConsumeTime);
             if(earliestErrorConsumeTime!=null){
                 if(earliestErrorConsumeTime.before(lastConsumeTime)){
                	 earliestDate = earliestErrorConsumeTime;
                 }else{
                	 earliestDate = lastConsumeTime;
                 }
             }else{
            	 earliestDate = lastConsumeTime;
             }
    	}
    	
    		 logger.info(String.format("{ %s } the earliestDate is  "+earliestDate, sellerId));
			
    	 if (startDate == null || startDate.before(earliestDate)) {
             startDate = earliestDate;
         }
//    	Date endDate = DateUtils.nextDays(new Date(), -1);
    	SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd");
        Date dt = new Date();
        String dy = formatDate.format(dt);
      	String ed =DateUtils.beginOfDay(dy);
      	Date endDate = DateUtils.str2Date(ed);
        if (logger.isInfoEnabled()) {
              logger.info(String.format("execute seller :{ %s } during【%s】", sellerId,
                      DateUtils.formatDate(endDate)));
        }
        for (Date date = startDate; date.before(endDate);) {
	        Day day = new Day(date);
	        calculateData(sellerId, day);
	        try{
	        	Thread.sleep(1000);
	        }catch(InterruptedException e){
	        	Thread.interrupted();
	        }
	        date = DateUtils.nextDays(date, 1);
        }
    }

    private void calculateData(Long sellerId, Day day) {
        Map<String, String> consumeFromMap = ConsumeCalculate.consumeFrom;

        List<String> keys = new ArrayList<String>(consumeFromMap.keySet());

        for (int i = 0; i < keys.size(); i++) {
            String consumeFrom = (String) keys.get(i);
            /*
             * 需做判断，因为有迁移表，目前只有主动营销会.type觉得查哪张表
             * 如果当天有任意活动正在迁移，则跳过这天。因为迁移只会在短信发送时间至少1天后才会进行。此时已经算好。除非数据错误，那也会在迁移完成后重算。
             * 当天所有主动营销活动迁移完成就历史表，否则就原先表。部分在迁移就跳过
             * 对于营销活动发送时间在今天，短信发送部分在明天才开始。这种就有问题，所以还不能跳过，要插入一条状态CONSUME_CALCULATE_FAILED的数据
             */
            if(consumeFrom.equals("115")||consumeFrom.equals("116")){
                Integer consumeType = BaseType.CONSUME_TYPE_SMS;
                if(consumeFrom.equals("116")){
                    consumeType = BaseType.CONSUME_TYPE_EMAIL;
                }
                List<ConsumeLogDo> consumeLoglist = this.consumeLogDao.loadConsumeDay(sellerId,
                        Day.getDayAsString(day), consumeType, consumeFrom);
                ConsumeLogDo consumeError = null;
                ConsumeLogDo consumeCal = new ConsumeLogDo();
                consumeCal.setSellerId(sellerId);
                consumeCal.setConsumeFrom(consumeFrom);
                consumeCal.setConsumeTime(day.getDayAsDate());
                consumeCal.setConsumeType(consumeType);
                
                if (consumeLoglist != null && consumeLoglist.size() > 0) {
                    for (int c = 1; c < consumeLoglist.size(); c++) {
                        consumeLogDao.delete(consumeLoglist.get(c).getId());
                    }
                    consumeError = consumeLoglist.get(0);
                }
                
                ActivityInfo actInfo = new ActivityInfo();
                String beginDate1 = Day.getDayAsString(day) + " 00:00:00";
                String beginDate2 = Day.getDayAsString(day) + " 23:59:59";
                actInfo.setSellerId(sellerId);
                actInfo.setActivityType(consumeFrom);
                actInfo.setStatus(0);
                actInfo.setCurrentStatus(2);
                actInfo.setCreated1(beginDate1);
                actInfo.setCreated2(beginDate2);
                actInfo.setIsHistory(1);
                Integer qianyiDoing = this.activityInfoDao.queryActivityInfoCountByHistory(actInfo);
                if(qianyiDoing!=null&&qianyiDoing>0){
                    //有个活动正在迁移，则记录时间，后面重算
                    if (consumeError == null) {
                        consumeCal.setStatus(BaseType.CONSUME_CALCULATE_FAILED);
                        consumeCal.setGmtCreated(new Date());
                        this.consumeLogDao.create(consumeCal);
                    } else {
                        consumeError.setStatus(BaseType.CONSUME_CALCULATE_FAILED);
                        consumeError.setGmtModified(new Date());
                        this.consumeLogDao.update(consumeError);
                    }
                }else{
                    Integer qianyiFinish=0;
                    Integer qianyiNo=0;
                    actInfo.setIsHistory(2);
                    qianyiFinish = this.activityInfoDao.queryActivityInfoCountByHistory(actInfo);
                    actInfo.setIsHistory(0);
                    qianyiNo = this.activityInfoDao.queryActivityInfoCountByHistory(actInfo);
                    
                    if(qianyiFinish==0&&qianyiNo>0){
                        //所有活动都没迁移
                        if(consumeFrom.equals("116")){
                            calculateDataForEmail(sellerId, consumeFrom, day,"1");
                        }else{
                            calculateDataForSms(sellerId, consumeFrom, day,"1");
                        }
                       
                    }else if(qianyiNo==0&&qianyiFinish>0){
                        //所有活动迁移完成
                        if(consumeFrom.equals("116")){
                            calculateDataForEmail(sellerId, consumeFrom, day,"2");
                        }else{
                            calculateDataForSms(sellerId, consumeFrom, day,"2");
                        }
                    }else if(qianyiNo>0&&qianyiFinish>0){
                        //部分迁移完成，部分没迁移，则记录时间，后面重算
                        if (consumeError == null) {
                            consumeCal.setStatus(BaseType.CONSUME_CALCULATE_FAILED);
                            consumeCal.setGmtCreated(new Date());
                            this.consumeLogDao.create(consumeCal);
                        } else {
                            consumeError.setStatus(BaseType.CONSUME_CALCULATE_FAILED);
                            consumeError.setGmtModified(new Date());
                            this.consumeLogDao.update(consumeError);
                        }
                    }else{
                        //当前没有活动，就干脆默认
                        if(consumeFrom.equals("116")){
                            calculateDataForEmail(sellerId, consumeFrom, day,"1");
                        }else{
                            calculateDataForSms(sellerId, consumeFrom, day,"1");
                        }
                    }
                    
                }
            }else{
                calculateDataForSms(sellerId, consumeFrom, day,"1");
                calculateDataForEmail(sellerId, consumeFrom, day,"1");                
            }
//            calculateDataForSms(sellerId, consumeFrom, day);
//            calculateDataForEmail(sellerId, consumeFrom, day);

        }

    }

    /**
     * 计算短信的费用
     * 如果没消耗短信，也要补足记录，否则会每天都进行不必要的计算而导致计算很慢
     */
    private void calculateDataForSms(Long sellerId, String consumeFrom, Day day,String type) {
        long start = System.currentTimeMillis();
        List<ConsumeLogDo> consumeLoglist = this.consumeLogDao.loadConsumeDay(sellerId,
                Day.getDayAsString(day), BaseType.CONSUME_TYPE_SMS, consumeFrom);
        ConsumeLogDo consume = null;
        ConsumeLogDo consumeCal = null;
        boolean doCal = Boolean.TRUE;
        if (consumeLoglist != null && consumeLoglist.size() > 0) {
            for (int i = 1; i < consumeLoglist.size(); i++) {
                consumeLogDao.delete(consumeLoglist.get(i).getId());
            }
            consume = consumeLoglist.get(0);
            doCal = filterNoCalulate(consume);
        }

        if (doCal) {
            //更新该记录状态为进行中
            if (consume != null) {
                consume.setStatus(BaseType.CONSUME_CALCULATE_DOING);
                consume.setGmtModified(new Date());
                this.consumeLogDao.update(consume);
            }
            try {
                //获取短信、邮件单价，默认短信单价5分，邮件单价1分
                CompanyDo companyDo = getCompanyDoForPrice(sellerId);
                
//                SmsLog smsLogCal = new SmsLog();
                Date created1 = day.getBeginOfDayAsDate();//Day.getDayAsString(day) + " 00:00:00";
                Date created2 = day.getEndOfDayAsDate(); //Day.getDayAsString(day) + " 23:59:59";
                int k = 0;
                Integer pageNo = 1;
                SmsLogDo smsLogDo = new SmsLogDo();
                smsLogDo.setSellerId(sellerId);
                smsLogDo.setStatus(1);
                /*
                smsLogCal.setCreated1(created1);
                smsLogCal.setCreated2(created2);
                smsLogCal.setSellerId(sellerId);
                smsLogCal.setStatus(1);
                smsLogCal.setEventType(consumeFrom);
                smsLogCal.setPageSize(5000);
                */
                PageQuery pageQuery = new PageQuery(1, 5000);
                while (true) {
                	/*
                    smsLogCal.setPageNo(pageNo);
                    DataList smsLogList = new DataList();
                    */
                	PageList<SmsLogDo> smsLogList = null;
                    if(type!=null&&type.equals("2")){
//                        smsLogList = this.userDao.querySmsLogHistoryDataList(smsLogCal);
                    	smsLogList = this.smsLogDao.listHistorySmsLogByStatusAndCreate(sellerId, created1, created2, Integer.valueOf(1), null, consumeFrom, pageQuery);
                    }else{
//                        smsLogList = this.userDao.querySmsLogDataList(smsLogCal);
                    	smsLogList = this.smsLogDao.listSmsLogByStatusAndCreate(sellerId, created1, created2, Integer.valueOf(1), null, consumeFrom, pageQuery);
                    }
                    

                    if (smsLogList == null || smsLogList.getPaginator().getItems() == 0) {
                    	consumeCal = doConsumeDefault(BaseType.CONSUME_TYPE_SMS, consumeFrom, day, consumeCal, companyDo,sellerId);
                        break;
                    }

//                    List<SmsLog> smsLogs = smsLogList.getData();
                    
                    for (SmsLogDo sms : smsLogList) {
                        consumeCal = doCalculateSms(sms, day, consumeCal, companyDo);
                        ++k;
                    }
                    logger.info(String.format("execute seller :{ %s } the smsSize is【%s】Finish", sellerId,
                    		smsLogList.size()));
                    
                    if(smsLogList.getPaginator().getNextPage()==pageQuery.getPageNum())
                    	break;
                   
                    pageQuery.increasePageNum();
                    pageQuery.setTotalCount(smsLogList.getPaginator().getItems());
                }
                //如果存在消耗短信记录则开始插入表中、此时如果表中有记录则更新，无记录则插入
                if (consumeCal != null) {
                    if (consume == null) {
                        consumeCal.setStatus(BaseType.CONSUME_CALCULATE_FINISH);
                        consumeCal.setGmtCreated(new Date());
                        this.consumeLogDao.create(consumeCal);
                    } else {
                        consumeCal.setId(consume.getId());
                        consumeCal.setStatus(BaseType.CONSUME_CALCULATE_FINISH);
                        consumeCal.setGmtModified(new Date());
                        this.consumeLogDao.update(consumeCal);
                    }
              
                    if (logger.isInfoEnabled()) {
                        logger.info(String.format(
                                "### {Thread(%s)}{%s【%s】} day(%s) smslogs size(%s) times(%s)-ms \t ", Thread
                                        .currentThread().getId(), sellerId, consumeFrom, day, k,
                                (System.currentTimeMillis() - start)));
                    }
                }

            } catch (Exception e) {
                logger.error("", e);
                if (consume == null) {
                	if(consumeCal != null) {
	                    consumeCal.setStatus(BaseType.CONSUME_CALCULATE_FAILED);
	                    consumeCal.setGmtCreated(new Date());
	                    this.consumeLogDao.create(consumeCal);
                	}
                } else {
                	if(consumeCal != null){
	                    consumeCal.setId(consume.getId());
	                    consumeCal.setStatus(BaseType.CONSUME_CALCULATE_FAILED);
	                    consumeCal.setGmtModified(new Date());
	                    this.consumeLogDao.update(consumeCal);
                	}
                }
                logger.error(String.format("XXXXXXXXXXXXXXX（%s）短信消费记录计算失败。 error info >  ",
                        Arrays.toString(e.getStackTrace())));
            }
        }
    }

    //补足单天没有消费短信。或者邮件的记录，默认记录为主动营销。
    private ConsumeLogDo doConsumeDefault(Integer type,String activityType, Day day,
			ConsumeLogDo consume, CompanyDo companyDo,Long sellerId) {
    	if (consume == null) {
            consume = new ConsumeLogDo();
        }
    	consume.setSellerId(sellerId);
    	consume.setConsumeFrom(activityType);
		 
        consume.setConsumeType(type);
       
        consume.setConsumeTime(day.getDayAsDate());
        consume.setConsumeCount(0l);
        consume.setConsumePrice(BigDecimal.ZERO);
        return consume;
	}

	private ConsumeLogDo doCalculateSms(SmsLogDo sms, Day day, ConsumeLogDo consume,
                                        CompanyDo companyDo) {
        if (consume == null) {
            consume = new ConsumeLogDo();
        }
        consume.setSellerId(sms.getSellerId());
        consume.setConsumeFrom(sms.getEventType());
        consume.setConsumeType(BaseType.CONSUME_TYPE_SMS);
        consume.setConsumeTime(day.getDayAsDate());

        if (sms != null) {
            String per = "0.05";
            //公司短消息价格可能改动，记录的是最新的短信价格，之前的历史价格无法获取，算历史短信价格可能会有差异
            if (companyDo != null && companyDo.getSmsPrice() != null
                    && !companyDo.getSmsPrice().equals("0.00000")) {
                per = companyDo.getSmsPrice().toString();
            }
            if (per.equals("0.00000")) {
                per = "0.05";
            }
            if (sms.getSmsNumber() == null) {
                if (sms.getSmsContent() != null && sms.getSmsContent().length() > 0) {
                	//67个字符算一条短信
                    int num = (sms.getSmsContent().length() / 67);
                    if (sms.getSmsContent().length() % 67 > 0) {
                        num = num + 1;
                    }
                    sms.setSmsNumber(num);
                }
            }
        
            if(sms.getSmsNumber()==null){
                sms.setSmsNumber(0);
            }
            consume.setConsumeCount(add(Long.valueOf(sms.getSmsNumber() + ""),
                    consume.getConsumeCount()));
            BigDecimal price = new BigDecimal(sms.getSmsNumber() + "")
                    .multiply(new BigDecimal(per)).setScale(4,BigDecimal.ROUND_HALF_UP);
            consume.setConsumePrice(add(consume.getConsumePrice(), price));
        }

        return consume;
    }

    /**
     * 计算邮件的费用
     */
    private void calculateDataForEmail(Long sellerId, String consumeFrom, Day day,String type) {
        long start = System.currentTimeMillis();
        List<ConsumeLogDo> consumeLoglist = this.consumeLogDao.loadConsumeDay(sellerId,
                Day.getDayAsString(day), BaseType.CONSUME_TYPE_EMAIL, consumeFrom);
        ConsumeLogDo consume = null;
        ConsumeLogDo consumeCal = null;
        boolean doCal = Boolean.TRUE;
        if (consumeLoglist != null && consumeLoglist.size() > 0) {
            for (int i = 1; i < consumeLoglist.size(); i++) {
                consumeLogDao.delete(consumeLoglist.get(i).getId());
            }
            consume = consumeLoglist.get(0);
            doCal = filterNoCalulate(consume);
        }

        if (doCal) {
            //更新该记录状态为进行中
            if (consume != null) {
                consume.setStatus(BaseType.CONSUME_CALCULATE_DOING);
                consume.setGmtModified(new Date());
                this.consumeLogDao.update(consume);
            }
            try {
                //获取短信、邮件单价，默认短信单价5分，邮件单价1分
                CompanyDo companyDo = getCompanyDoForPrice(sellerId);

                EmailSentDo emailSentCal = new EmailSentDo();
                String created1 = Day.getDayAsString(day) + " 00:00:00";
                String created2 = Day.getDayAsString(day) + " 23:59:59";
                int k = 0;
                emailSentCal.setSellerId(sellerId);
                emailSentCal.setStatus(1);
                emailSentCal.setActivityType(consumeFrom);
                PageQuery pageQuery = new PageQuery(0, 5000);
                while (true) {
                    PageList<EmailSentDo> emailSentlist = null;
                    if(type!=null&&type.equals("2")){
                        emailSentlist = emailSentDao.listEmailByPageHistory(
                                emailSentCal, created1, created2, pageQuery);
                    }else{
                        emailSentlist = emailSentDao.listEmailByPage(
                                emailSentCal, created1, created2, pageQuery);
                    }
                    
                    if (emailSentlist == null || emailSentlist.isEmpty()) {
                    	consumeCal = doConsumeDefault(BaseType.CONSUME_TYPE_EMAIL,consumeFrom, day, consumeCal, companyDo,sellerId);
                        break;
                    }

                    Paginator paginator = emailSentlist.getPaginator();

                    for (EmailSentDo emailSent : emailSentlist) {
                        consumeCal = doCalculateEmail(emailSent, day, consumeCal, companyDo);
                        ++k;
                    }

                    if (paginator.getNextPage() == paginator.getPage()) {
                        break;
                    }

                    pageQuery.increasePageNum();

                }
                //如果存在消耗邮件记录则开始插入表中、此时如果表中有记录则更新，无记录则插入
                if (consumeCal != null) {
                    if (consume == null) {
                        consumeCal.setStatus(BaseType.CONSUME_CALCULATE_FINISH);
                        consumeCal.setGmtCreated(new Date());
                        this.consumeLogDao.create(consumeCal);
                    } else {
                        consumeCal.setId(consume.getId());
                        consumeCal.setStatus(BaseType.CONSUME_CALCULATE_FINISH);
                        consumeCal.setGmtModified(new Date());
                        this.consumeLogDao.update(consumeCal);
                    }
                
                    if (logger.isInfoEnabled()) {
                        logger.info(String.format(
                                "###  {Thread(%s)}{%s【%s】} day(%s) emailsents size(%s) times(%s)-ms \t ", Thread
                                .currentThread().getId(),sellerId,
                                consumeFrom, day, k, (System.currentTimeMillis() - start)));
                    }
                }

            } catch (Exception e) {
                logger.error("", e);
                if (consume == null) {
                    consumeCal.setStatus(BaseType.CONSUME_CALCULATE_FAILED);
                    consumeCal.setGmtCreated(new Date());
                    this.consumeLogDao.create(consumeCal);
                } else {
                    consumeCal.setId(consume.getId());
                    consumeCal.setStatus(BaseType.CONSUME_CALCULATE_FAILED);
                    consumeCal.setGmtModified(new Date());
                    this.consumeLogDao.update(consumeCal);
                }
                logger.error(String.format("XXXXXXXXXXXXXXX（%s）邮件消费记录计算失败。 error info >  ",
                        Arrays.toString(e.getStackTrace())));
            }
        }
    }

    private ConsumeLogDo doCalculateEmail(EmailSentDo emailSent, Day day, ConsumeLogDo consume,
                                          CompanyDo companyDo) {
        // TODO Auto-generated method stub
        if (consume == null) {
            consume = new ConsumeLogDo();
        }
        consume.setSellerId(emailSent.getSellerId());
        consume.setConsumeFrom(emailSent.getActivityType());
        consume.setConsumeType(BaseType.CONSUME_TYPE_EMAIL);
        consume.setConsumeTime(day.getDayAsDate());

        if (emailSent != null) {
            String per = "0.01";
            if (companyDo != null && companyDo.getEmailPrice() != null
                    && !companyDo.getEmailPrice().equals("0.00000")) {
                per = companyDo.getEmailPrice().toString();
            }
            if (per.equals("0.00000")) {
                per = "0.01";
            }
            consume.setConsumeCount(add(1L, consume.getConsumeCount()));
            consume.setConsumePrice(add(consume.getConsumePrice(), new BigDecimal(per)));
        }

        return consume;
    }

    /**
     * 过滤正在计算中的、计算完成的、不计算重算的
     * 
     * @param consumeLoglist
     * @return
     */
    private boolean filterNoCalulate(ConsumeLogDo consume) {
        if (consume.getStatus().intValue() == BaseType.CONSUME_CALCULATE_DOING
                || consume.getStatus().intValue() == BaseType.CONSUME_CALCULATE_FINISH
                || consume.getStatus().intValue() == BaseType.CONSUME_CALCULATE_JOIN) {
            return false;
        }
        return true;
    }

    private CompanyDo getCompanyDoForPrice(Long sellerId) {
        CompanyDo companyDo = new CompanyDo();
        List<CompanySellerDo> companySellerDolist = this.companyDao.listCompanyIdForPrice(sellerId);
        if (companySellerDolist != null && companySellerDolist.size() > 0) {
            CompanySellerDo companySellerDo = companySellerDolist.get(0);
            companyDo = this.companyDao.load(companySellerDo.getCompanyId());
        }
        return companyDo;
    }

    public static void main(String[] args) {

    }

}
