package com.wangjubao.app.others.dayreport;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.web.servlet.view.freemarker.FreeMarkerConfigurer;

import com.wangjubao.core.util.SynContext;
import com.wangjubao.dolphin.biz.common.constant.SellerType;
import com.wangjubao.dolphin.biz.common.model.ServiceResponse;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.biz.service.SellerDayReportService;

import com.wangjubao.framework.util.DateUtil;
import com.wangjubao.framework.util.StrUtil;
import com.wangjubao.service.email.EmailSendService;

import freemarker.template.Configuration;
import freemarker.template.TemplateException;

public class SellerDayReport {
//    static final Logger            logger = Logger.getLogger(SellerDayReport.class);
    public transient static Logger        dayreprot = Logger.getLogger("dayreport");
    
    private SellerDayReportService sellerDayReportService;
//    private IMailService           mailService;
    private FreeMarkerConfigurer freemarkerConfig;
    private EmailSendService emailSendService;

    public SellerDayReport() {
        sellerDayReportService = (SellerDayReportService) SynContext.getObject("sellerDayReportService");
//        mailService = (IMailService) SynContext.getObject("mailService");
        freemarkerConfig = (FreeMarkerConfigurer)SynContext.getObject("freemarkerConfig");
        emailSendService = (EmailSendService)SynContext.getObject("emailSendDubboService");
    }

    public boolean SendDayReport(SellerDo seller) {
        Long sellerId = seller.getId();
        if (seller.getNotifyEmails() == null || StrUtil.isEmpty(seller.getNotifyEmails().trim())) {
          return true;
        }
        dayreprot.info(seller.getNick() + ", start to query trade info");
        Map<String, Object> dayInfMap = sellerDayReportService.queryEveryDayReportToUser(sellerId);
        if (dayInfMap.size() == 0) {
            return true;
        }
        dayreprot.info(seller.getNick() + ", query trade info finished");
        
        Date date = DateUtil.nextDays(-1);

        dayInfMap.put("user", seller);
        dayInfMap.put("nick", seller.getNick());
        dayInfMap.put("email", seller.getNotifyEmails());
        dayInfMap.put("sellerType", seller.getSellerType());
        
        //		String emails="shell_tao@qq.com;375235133@qq.com;33362253@qq.com"; 
        //		dayInfMap.put("email",emails );

        boolean retb = false;

//        dayreprot.info(String.format("{%s【%s】} 日报发送 【%s】,接收邮箱列表: %s ", seller.getId(),
//                seller.getNick(), date, seller.getNotifyEmails()));

        retb = sendEveryDayStat(sellerId, dayInfMap, date);

//        dayreprot.info(String.format("{%s【%s】} 日报发送 【%s】,接收邮箱列表: %s ", seller.getId(),
//                seller.getNick(), retb == true ? "成功" : "失败", seller.getNotifyEmails()));
        //        		logger.info("---------------------" + emails+"----发送成功否："+retb);

        return retb;
    }
    
    public boolean sendEveryDayStat(Long sellerId, Map<String, Object> map, Date date) {
        String nick = map.get("nick").toString();
        String emails = map.get("email").toString();
        try {
            ClassLoader.getSystemResource("/");

            Map<String, Object> root = map;
            String html = getFreemarkerHtml(root, "/mail/everyStat.ftl");
            String sellerType = null;
            String subject = null;
            if(map.get("sellerType") != null){
                sellerType = SellerType.getTypeName(Integer.parseInt( map.get("sellerType").toString()));
                subject = "["+sellerType+"]"+nick + "：店铺运营日报[" + DateUtil.format(date, "MM月dd日") + "]";
            }else{
            	subject = nick + "：店铺运营日报[" + DateUtil.format(date, "MM月dd日") + "]";
            }

            dayreprot.info(nick+", invoke email service to send report");
            Integer sleep = 5;
            String[] es = emails.split(";");
            ServiceResponse<Void> response = emailSendService.sendFree(sellerId, es, subject, html);
            if(response.isSuccess()){
            	dayreprot.info(nick+", send daily report SUCCESS to : "+emails);
            	return true;
            }else{
            	dayreprot.info(nick+", send report mail FAIL, error:" + response.getErrorMessage());
            }
/*
            for (String email : es) {
                if (StrUtil.isNotEmpty(email)) {
                    helper.setTo(email);
                    dayreprot.info(subject+" to :" + email);
                    // 前期发布版本保障测试
                    Thread.sleep(sleep * 1000);
                    new MailSendThread(mailSender, mimeMessage).run();
                }
            }

            return true;
  */
        } catch (Exception e) {
        	dayreprot.error(nick+", send daily report FAIL to : "+emails);
            dayreprot.error("send error mail is failed", e);
        }
        return false;
    }
    
    private String getFreemarkerHtml(Map<String, Object> root, String ftl)
            throws TemplateException, IOException {
        Configuration cfg = freemarkerConfig.getConfiguration();
        // this.getFreemarkerConfig().
        // cfg.setLocalizedLookup(false);
        java.io.StringWriter stringWriter = new StringWriter();
        Writer out = new BufferedWriter(stringWriter);
        // TemplateLoader locader = cfg.getTemplateLoader();
        // cfg.setTemplateLoader(new FileTemplateLoader(new File(str)));
        cfg.getTemplate(ftl).process(root, out);
        // cfg.setLocalizedLookup(true);
        // cfg.setTemplateLoader(locader);
        return stringWriter.toString();
    }
}
