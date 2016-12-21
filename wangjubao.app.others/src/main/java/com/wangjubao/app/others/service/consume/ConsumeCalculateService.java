package com.wangjubao.app.others.service.consume;

import java.text.ParseException;
import java.util.Date;

/**
 * 短信邮件消费计算
 * @author luorixin
 *
 */
public interface ConsumeCalculateService {


    /**
     * 从指定日期开始计算某个卖家的短信邮件消费
     * 
     * @param sellerId
     * @param startDate 如果为null则取最早的t_sms_log或t_email_sent日期
     * @throws ParseException 
     */
    void calculateConsumeForDate(Long sellerId, Date startDate) throws ParseException;
    
}
