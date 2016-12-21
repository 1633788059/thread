package com.wangjubao.app.others.service.downcenter.impl;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wangjubao.core.domain.marketing.ActivityInfo;
import com.wangjubao.core.service.marketing.IActivityInfoService;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dao.EmailSentDao;
import com.wangjubao.dolphin.biz.model.EmailSentDo;
import com.wangjubao.dolphin.biz.model.OrderDo;
import com.wangjubao.dolphin.biz.model.SysSyntaskDo;
import com.wangjubao.dolphin.biz.model.TradeDo;
import com.wangjubao.dolphin.biz.oss.OSSAccessClient;
import com.wangjubao.dolphin.biz.oss.OSSResponse;
import com.wangjubao.dolphin.biz.service.OrderService;
import com.wangjubao.dolphin.biz.service.SellerEmailSendService;
import com.wangjubao.dolphin.biz.service.SellerService;
import com.wangjubao.dolphin.biz.service.TradeService;
import com.wangjubao.dolphin.common.lang.Paginator;
import com.wangjubao.dolphin.common.util.JodaTime;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.date.DateUtils;
import com.wangjubao.framework.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.quote.AlwaysQuoteMode;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by bama on 2016/11/29.
 */
@Service("marketingReportDownService")
public class MarketingReportDownServiceImpl extends AbstractDownCenterServiceImpl{

    static Logger logger = LoggerFactory.getLogger("downcenter");

    private static final String PARENTFILE="marketingReportDown/";

    OSSAccessClient ossAccessClient = new OSSAccessClient();

    private  String path="";

    private static String serverIp="0.0.0.0";

    @Autowired
    private SellerService sellerService;

    @Autowired
    private TradeService tradeService;
    @Autowired
    private OrderService orderService;
    @Autowired
    private IActivityInfoService activityInfoService;
    @Autowired
    private SellerEmailSendService sellerEmailSendService;
    @Autowired
    private EmailSentDao emailSentDao;

    @Override
    public void init(SysSyntaskDo sysSyntaskDo) {
        sysSyntaskDo.setStatus("init");
        sellerService.updateSysSyntaskDo(sysSyntaskDo);
    }

    @Override
    public void job(SysSyntaskDo task) {
        path ="conf/ExportMarketingReport.csv";
        try {
            InetAddress addr = InetAddress.getLocalHost();
            serverIp=addr.getHostAddress().toString(); //获取本机ip

        } catch (Exception e) {

        }

        try {
            task.setOwnerId(serverIp);
            task.setStatus("doing");

            createCSVData(task.getSellerId(),task);
        } catch (Exception e) {
            task.setStatus("fail");
            task.setData(e.getMessage());
            e.printStackTrace();
        }finally{
            sellerService.updateSysSyntaskDo(task);
        }
    }

    private synchronized  void createCSVData(Long sellerId,SysSyntaskDo task) {
        /*解析下载任务参数*/
        String jsonStr = task.getParam();
        logger.info("##>>MarketingReportDown>>>" + jsonStr);
        JsonParser parser = new JsonParser();
        JsonObject json = parser.parse(jsonStr).getAsJsonObject();
        String opt=null;//效果报告类型
        JsonElement optElement = json.get("opt");
        if(optElement==null){
            task.setStatus("fail");
            task.setData("opt is null");
            logger.info("markegint-report-opt is null");
            return;
        }
        opt=optElement.getAsString();
        /*创建文件*/
        String now = JodaTime.formatDate(new Date(), "yyyyMMddHHmmss");
        String fileName = new StringBuilder("Market_Report").append(task.getSellerId()).append('_').append(task.getId())
                .append('_').append(now).append(".csv").toString();
        path = "conf/" + fileName;
        final File file = new File(path);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e1) {
                logger.error(e1.getMessage());
            }
        }
        if("payorder".equals(opt)){
            writeOrderData(sellerId,task,file,"1");
        }else if("nopayorder".equals(opt)){
            writeOrderData(sellerId,task,file,"0");
        }else if("paygoods".equals(opt)){
            writeGoodsData(sellerId,task,file,"1");
        }else if("nopaygoods".equals(opt)){
            writeGoodsData(sellerId,task,file,"0");
        }else if("emailloglist".equals(opt)){
            writeEmailLogData(sellerId,task,file);
        }else{
            logger.info("{}:opt--{}",DateUtils.formatDate(new Date(),"yyyy-MM-dd HH:mm:ss"),opt);
        }
    }
    /*order*/
    private synchronized void writeOrderData(Long sellerId,SysSyntaskDo task,File file,String isPay){
        CsvListWriter csvWriter = null;
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(file, "GBK");
            CsvPreference.Builder preferBuilder = new CsvPreference.Builder(CsvPreference.EXCEL_PREFERENCE)
                    .useQuoteMode(new AlwaysQuoteMode());
            CsvPreference csvPreference = preferBuilder.build();
            csvWriter = new CsvListWriter(pw, csvPreference);
            /*查询参数*/
            String jsonStr = task.getParam();
            JsonParser parser = new JsonParser();
            JsonObject json = parser.parse(jsonStr).getAsJsonObject();

            JsonElement  jsonElement=json.get("buyerNick");
            String buyerNick=null;
            if(jsonElement!=null){
                buyerNick=jsonElement.getAsString()==""?null:jsonElement.getAsString();
            }
            jsonElement=json.get("receiverMobile");
            String receiverMobile=null;
            if(jsonElement!=null){
                receiverMobile=jsonElement.getAsString()==""?null:jsonElement.getAsString();
            }
            jsonElement=json.get("email");
            String email=null;
            if(jsonElement!=null){
                email=jsonElement.getAsString()==""?null:jsonElement.getAsString();
            }
            jsonElement=json.get("activityId");
            String activityId=null;
            if(jsonElement!=null){
                activityId=jsonElement.getAsString()==""?null:jsonElement.getAsString();
            }
            jsonElement=json.get("groupName");
            String groupName="所有";
            if(jsonElement!=null){
                groupName=jsonElement.getAsString()==""?null:jsonElement.getAsString();
            }
            if(activityId==null){
                task.setStatus("fail");
                task.setData("activityId is null");
                logger.info("markegint-report-activityId is null");
                return;
            }
            String eventIds="";
            ActivityInfo activityInfo=new ActivityInfo();
            activityInfo.setSellerId(sellerId);
            activityInfo.setActivityId(Long.valueOf(activityId));
            activityInfo=activityInfoService.queryActivityInfoById(activityInfo);
            if (activityInfo.getFilterType() != null && activityInfo.getFilterType().intValue() == 2 && activityInfo.getParentId() == null) {
                ActivityInfo actParents = new ActivityInfo();
                actParents.setSellerId(sellerId);
                actParents.setActivityType(activityInfo.getActivityType());
                actParents.setParentId(activityInfo.getActivityId());
                List<ActivityInfo> parents = activityInfoService.queryActivityInfoList(actParents);

                for (ActivityInfo actP : parents) {
                    eventIds = eventIds + "'" + actP.getActivityId().toString() + "',";
                }
            }
            TradeDo trade=new TradeDo();
            if (eventIds != "") {
                eventIds = eventIds.substring(0, eventIds.length() - 1);
                trade.setBuyerMemo(eventIds);
                trade.setBuyerMessage(null);
            } else {
                trade.setBuyerMessage(activityInfo.getActivityId().longValue()+"");
            }
            trade.setSellerId(sellerId);
            if (buyerNick != null && buyerNick != "") {
                trade.setBuyerNick(buyerNick);
            }
            if (receiverMobile != null && receiverMobile != "") {
                trade.setReceiverMobile(receiverMobile);
            }
            if (email != null && email != "") {
                trade.setBuyerEmail(email);
            }
            PageQuery pageQuery = new PageQuery(1, 200);
            Integer count=null;
             /*写入文件头部*/
            String header = "买家昵称,手机号码,交易金额,付款时间,营销分组";
            String type = "sms";//效果报告类型
            JsonElement typeElement = json.get("type");
            if ("email".equals(type)) {
                header = "买家昵称,邮件地址，交易金额，付款时间,营销分组";
            }
            csvWriter.writeHeader(header.split(","));

            while(true) {
                PageList<TradeDo> pageList = new PageList();
                /*查询订单*/
                if ("1".equals(isPay)) {
                    trade.setTradeMemo(1+"");
                    pageList = tradeService.queryPayTradeList(sellerId,trade,pageQuery,count);
                } else if ("0".equals(isPay)) {
                    trade.setTradeMemo(0+"");
                    pageList = tradeService.queryPayTradeList(sellerId,trade,pageQuery,count);
                }

                /*写入订单内容*/
                for (TradeDo tradeDo : pageList) {
                    List<String> rowWrite = new ArrayList<String>();
                    rowWrite.add(tradeDo.getBuyerNick() == null ? "" : tradeDo.getBuyerNick().toString());
                    if (!"email".equals(type)) {
                        rowWrite.add(tradeDo.getReceiverMobile() == null ? "" : tradeDo.getReceiverMobile().toString());
                    }else{
                        rowWrite.add(tradeDo.getBuyerEmail() == null ? "" : tradeDo.getBuyerEmail().toString());
                    }
                    rowWrite.add(tradeDo.getPayment() == null ? "" : tradeDo.getPayment().setScale(2, BigDecimal.ROUND_HALF_UP).toString());
                    if("1".equals(isPay)) {
                        rowWrite.add(tradeDo.getPayTime() == null ? "" : DateUtil.format(tradeDo.getPayTime(), "yyyy-MM-dd HH:mm:ss"));
                    }else{
                        rowWrite.add(tradeDo.getCreated() == null ? "" : DateUtil.format(tradeDo.getCreated(), "yyyy-MM-dd HH:mm:ss"));
                    }
                    rowWrite.add(groupName);
                    csvWriter.write(rowWrite);
                }
                Paginator paginator = pageList.getPaginator();
                if (paginator.getNextPage() == paginator.getPage())
                {
                    break;
                }
                count=paginator.getItems();
                pageQuery.increasePageNum();
            }
            csvWriter.close();
            final File finalFile = new File(path);
            String key = PARENTFILE + sellerId + "_" + activityId + "_" + System.currentTimeMillis() + ".csv.zip";
            File zipFile = ossAccessClient.zip(finalFile, path);
            OSSResponse rsp = ossAccessClient.put("wjbcrm", key, zipFile, DateUtils.nextDays(new Date(), 7));
            task.setTitle(task.getTitle());
            task.setData((rsp.getUrlOss() + "").replace("wjbcrm.oss-internal.aliyuncs.com","wjbcrm.oss.aliyuncs.com"));
            task.setDescription(task.getDescription() + "  key:" + rsp.getKeyVlaue());
            System.out.println(rsp.getUrlOss());
            task.setStatus("done");

        }catch(Exception e){
            task.setStatus("fail");
            task.setData(e.getMessage());
            logger.error(e.getMessage(), e);
        }
    }
    /*Goods*/
    private synchronized void writeGoodsData(Long sellerId,SysSyntaskDo task,File file,String isPay){
        CsvListWriter csvWriter = null;
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(file, "GBK");
            CsvPreference.Builder preferBuilder = new CsvPreference.Builder(CsvPreference.EXCEL_PREFERENCE)
                    .useQuoteMode(new AlwaysQuoteMode());
            CsvPreference csvPreference = preferBuilder.build();
            csvWriter = new CsvListWriter(pw, csvPreference);
            /*查询参数*/
            String jsonStr = task.getParam();
            JsonParser parser = new JsonParser();
            JsonObject json = parser.parse(jsonStr).getAsJsonObject();

            JsonElement  jsonElement=json.get("buyerNick");
            String buyerNick=null;
            if(jsonElement!=null){
                buyerNick=jsonElement.getAsString()==""?null:jsonElement.getAsString();
            }
            jsonElement=json.get("itemTitle");
            String itemTitle=null;
            if(jsonElement!=null){
                itemTitle=jsonElement.getAsString()==""?null:jsonElement.getAsString();
            }
            jsonElement=json.get("activityId");
            String activityId=null;
            if(jsonElement!=null){
                activityId=jsonElement.getAsString()==""?null:jsonElement.getAsString();
            }
            jsonElement=json.get("groupName");
            String groupName="所有";
            if(jsonElement!=null){
                groupName=jsonElement.getAsString()==""?null:jsonElement.getAsString();
            }
            if(activityId==null){
                task.setStatus("fail");
                task.setData("activityId is null");
                logger.info("markegint-report-activityId is null");
                return;
            }
            String eventIds="";
            ActivityInfo activityInfo=new ActivityInfo();
            activityInfo.setSellerId(sellerId);
            activityInfo.setActivityId(Long.valueOf(activityId));
            activityInfo=activityInfoService.queryActivityInfoById(activityInfo);
            if (activityInfo.getFilterType() != null && activityInfo.getFilterType().intValue() == 2 && activityInfo.getParentId() == null) {
                ActivityInfo actParents = new ActivityInfo();
                actParents.setSellerId(sellerId);
                actParents.setActivityType(activityInfo.getActivityType());
                actParents.setParentId(activityInfo.getActivityId());
                List<ActivityInfo> parents = activityInfoService.queryActivityInfoList(actParents);

                for (ActivityInfo actP : parents) {
                    eventIds = eventIds + "'" + actP.getActivityId().toString() + "',";
                }
            }
            OrderDo order=new OrderDo();
            if (eventIds != "") {
                eventIds = eventIds.substring(0, eventIds.length() - 1);
                order.setExtend3(eventIds);
                order.setExtend2(null);
            } else {
                order.setExtend2(activityInfo.getActivityId().longValue()+"");
            }
            order.setSellerId(sellerId);
            if (buyerNick != null && buyerNick != "") {
                order.setBuyerNick(buyerNick);
            }
            if (itemTitle != null && itemTitle != "") {
                order.setExtend1(itemTitle);
            }
            PageQuery pageQuery = new PageQuery(1, 200);
            Integer count=null;
             /*写入文件头部*/
            String header = "买家昵称,宝贝名称,宝贝价格,宝贝数量,付款金额,营销分组";
            csvWriter.writeHeader(header.split(","));

            while(true) {
                PageList<OrderDo> pageList = new PageList();
                /*查询订单*/
                if ("1".equals(isPay)) {
                    order.setStatus(1);
                    pageList = orderService.queryPayOrderList(sellerId,order,pageQuery,count);
                } else if ("0".equals(isPay)) {
                    order.setStatus(0);
                    pageList = orderService.queryPayOrderList(sellerId,order,pageQuery,count);
                }

                /*写入订单内容*/
                for (OrderDo orderDo : pageList) {
                    List<String> rowWrite = new ArrayList<String>();
                    rowWrite.add(orderDo.getBuyerNick() == null ? "" : orderDo.getBuyerNick().toString());
                    rowWrite.add(orderDo.getTitle() == null ? "" : orderDo.getTitle().toString());
                    rowWrite.add(orderDo.getPrice() == null ? "" : orderDo.getPrice().setScale(2, BigDecimal.ROUND_HALF_UP).toString());
                    rowWrite.add(orderDo.getNum() == null ? "" : orderDo.getNum().toString());
                    rowWrite.add(orderDo.getPayment() == null ? "" : orderDo.getPayment().setScale(2, BigDecimal.ROUND_HALF_UP).toString());
                    rowWrite.add(groupName);
                    csvWriter.write(rowWrite);
                }
                Paginator paginator = pageList.getPaginator();
                if (paginator.getNextPage() == paginator.getPage())
                {
                    break;
                }
                count=paginator.getItems();
                pageQuery.increasePageNum();
            }
            csvWriter.close();
            final File finalFile = new File(path);
            String key = PARENTFILE + sellerId + "_" + activityId + "_" + System.currentTimeMillis() + ".csv.zip";
            File zipFile = ossAccessClient.zip(finalFile, path);
            OSSResponse rsp = ossAccessClient.put("wjbcrm", key, zipFile, DateUtils.nextDays(new Date(), 7));
            task.setTitle(task.getTitle());
            task.setData((rsp.getUrlOss() + "").replace("wjbcrm.oss-internal.aliyuncs.com","wjbcrm.oss.aliyuncs.com"));
            task.setDescription(task.getDescription() + "  key:" + rsp.getKeyVlaue());
            System.out.println(rsp.getUrlOss());
            task.setStatus("done");

        }catch(Exception e){
            task.setStatus("fail");
            task.setData(e.getMessage());
            logger.error(e.getMessage(), e);
        }
    }
    /*emailloglist*/
    private synchronized void writeEmailLogData(Long sellerId,SysSyntaskDo task,File file){
        CsvListWriter csvWriter = null;
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(file, "GBK");
            CsvPreference.Builder preferBuilder = new CsvPreference.Builder(CsvPreference.EXCEL_PREFERENCE)
                    .useQuoteMode(new AlwaysQuoteMode());
            CsvPreference csvPreference = preferBuilder.build();
            csvWriter = new CsvListWriter(pw, csvPreference);
            /*查询参数*/
            String jsonStr = task.getParam();
            JsonParser parser = new JsonParser();
            JsonObject json = parser.parse(jsonStr).getAsJsonObject();
            JsonElement jsonElement=json.get("activityId");
            String activityId=null;
            if(jsonElement!=null){
                activityId=jsonElement.getAsString()==""?null:jsonElement.getAsString();
            }
            jsonElement=json.get("email");
            String email=null;
            if(jsonElement!=null){
                email=jsonElement.getAsString()==""?null:jsonElement.getAsString();
            }
            jsonElement=json.get("status");
            String status=null;
            if(jsonElement!=null){
                status=jsonElement.getAsString()==""?null:jsonElement.getAsString();
            }
            jsonElement=json.get("openStatus");
            String openStatus=null;
            if(jsonElement!=null){
                openStatus=jsonElement.getAsString()==""?null:jsonElement.getAsString();
            }
            jsonElement=json.get("buyerNick");
            String buyerNick=null;
            if(jsonElement!=null){
                buyerNick=jsonElement.getAsString()==""?null:jsonElement.getAsString();
            }



            if(activityId==null){
                task.setStatus("fail");
                task.setData("activityId is null");
                logger.info("markegint-report-activityId is null");
                return;
            }
            String eventIds="";
            ActivityInfo activityInfo=new ActivityInfo();
            activityInfo.setSellerId(sellerId);
            activityInfo.setActivityId(Long.valueOf(activityId));
            activityInfo=activityInfoService.queryActivityInfoById(activityInfo);
            if (activityInfo.getFilterType() != null && activityInfo.getFilterType().intValue() == 2 && activityInfo.getParentId() == null) {
                ActivityInfo actParents = new ActivityInfo();
                actParents.setSellerId(sellerId);
                actParents.setActivityType(activityInfo.getActivityType());
                actParents.setParentId(activityInfo.getActivityId());
                List<ActivityInfo> parents = activityInfoService.queryActivityInfoList(actParents);

                for (ActivityInfo actP : parents) {
                    eventIds = eventIds + "'" + actP.getActivityId().toString() + "',";
                }
            }

            PageQuery pageQuery = new PageQuery(1, 200);
            Integer count=null;
            EmailSentDo emailSent = new EmailSentDo();
            emailSent.setSellerId(sellerId);
            emailSent.setActivityType(activityInfo.getActivityType());
            if (eventIds != "") {
                eventIds = eventIds.substring(0, eventIds.length() - 1);
                emailSent.setActivityIds(eventIds);
                emailSent.setActivityId(null);
            } else {
                emailSent.setActivityId(activityInfo.getActivityId());
            }
            emailSent.setEmails(email);
            emailSent.setBuyerNick(buyerNick);
            if(status!=null) {
                emailSent.setStatus(Integer.valueOf(status));
            }
            if(openStatus!=null) {
                emailSent.setOpenStatus(Integer.valueOf(openStatus));
            }
            /*写入文件头部*/
            String header = "买家昵称,邮箱地址,发送时间,发送状态,是否打开";
            if(activityInfo.getActivityType().equals("135")){
                header = "邮箱地址,发送时间,发送状态,是否打开";
            }
            csvWriter.writeHeader(header.split(","));
            if(emailSent.getStatus()!=null&&emailSent.getStatus().intValue()==-999){
                emailSent.setStatus(5);
                String begin = DateUtils.formatDate(DateUtils.nextDays(activityInfo.getBeginDate(), -7), "yyyy-MM-dd") + " 00:00:00";
                String end = DateUtils.formatDate(DateUtils.nextDays(activityInfo.getBeginDate(), 3), "yyyy-MM-dd") + " 23:59:59";
                if(eventIds!=""){
                    ActivityInfo actParents = new ActivityInfo();
                    actParents.setSellerId(sellerId);
                    actParents.setActivityType(activityInfo.getActivityType());
                    actParents.setParentId(activityInfo.getActivityId());
                    List<ActivityInfo> parents = activityInfoService.queryActivityInfoList(actParents);

                    for (ActivityInfo actP : parents) {
                        while (true) {
                            PageList<EmailSentDo> pageList = new PageList();
                            pageList=emailSentDao.listEmailSentDos(emailSent,begin,end,actP.getActivityId(),actP.getActivityType(),pageQuery);
                            for (EmailSentDo emailSentDo : pageList) {
                                List<String> rowWrite = new ArrayList<String>();
                                if(!activityInfo.getActivityType().equals("135")){
                                    rowWrite.add(emailSentDo.getBuyerNick());
                                }
                                rowWrite.add(emailSentDo.getEmails());
                                rowWrite.add(DateUtils.formatDate(emailSentDo.getGmtCreated(), "yyyy-MM-dd HH:mm:ss"));
                                rowWrite.add(getEmailSendStatus(emailSentDo.getStatus()));
                                if (emailSentDo.getOpenStatus() != null) {
                                    rowWrite.add(emailSentDo.getOpenStatus().intValue() == 1 ? "是" : "否");
                                } else {
                                    rowWrite.add("否");
                                }
                                csvWriter.write(rowWrite);
                            }

                            Paginator paginator = pageList.getPaginator();
                            if (paginator.getNextPage() == paginator.getPage()) {
                                break;
                            }
                            count = paginator.getItems();
                            pageQuery.increasePageNum();
                        }
                    }
                }else{
                    while (true) {
                        PageList<EmailSentDo> pageList = new PageList();
                        pageList=emailSentDao.listEmailSentDos(emailSent,begin,end,activityInfo.getActivityId(),activityInfo.getActivityType(),pageQuery);
                        for (EmailSentDo emailSentDo : pageList) {
                            List<String> rowWrite = new ArrayList<String>();
                            if(!activityInfo.getActivityType().equals("135")){
                                rowWrite.add(emailSentDo.getBuyerNick());
                            }
                            rowWrite.add(emailSentDo.getEmails());
                            rowWrite.add(DateUtils.formatDate(emailSentDo.getGmtCreated(), "yyyy-MM-dd HH:mm:ss"));
                            rowWrite.add(getEmailSendStatus(emailSentDo.getStatus()));
                            if (emailSentDo.getOpenStatus() != null) {
                                rowWrite.add(emailSentDo.getOpenStatus().intValue() == 1 ? "是" : "否");
                            } else {
                                rowWrite.add("否");
                            }
                            csvWriter.write(rowWrite);
                        }

                        Paginator paginator = pageList.getPaginator();
                        if (paginator.getNextPage() == paginator.getPage()) {
                            break;
                        }
                        count = paginator.getItems();
                        pageQuery.increasePageNum();
                    }
                }
            }else {
                while (true) {
                    PageList<EmailSentDo> pageList = new PageList();
                    /*写入短信记录内容*/
                    if (activityInfo.getIsHistory() != null && activityInfo.getIsHistory().intValue() == 2) {
                        pageList = sellerEmailSendService.listByPageHistory(emailSent, pageQuery);
                    } else {
                        pageList = sellerEmailSendService.listByPage(emailSent, pageQuery);
                    }

                    for (EmailSentDo emailSentDo : pageList) {
                        List<String> rowWrite = new ArrayList<String>();
                        if(!activityInfo.getActivityType().equals("135")){
                            rowWrite.add(emailSentDo.getBuyerNick());
                        }
                        rowWrite.add(emailSentDo.getEmails());
                        rowWrite.add(DateUtils.formatDate(emailSentDo.getGmtCreated(), "yyyy-MM-dd HH:mm:ss"));
                        rowWrite.add(getEmailSendStatus(emailSentDo.getStatus()));
                        if (emailSentDo.getOpenStatus() != null) {
                            rowWrite.add(emailSentDo.getOpenStatus().intValue() == 1 ? "是" : "否");
                        } else {
                            rowWrite.add("否");
                        }
                        csvWriter.write(rowWrite);
                    }

                    Paginator paginator = pageList.getPaginator();
                    if (paginator.getNextPage() == paginator.getPage()) {
                        break;
                    }
                    count = paginator.getItems();
                    pageQuery.increasePageNum();
                }
            }


            csvWriter.close();
            final File finalFile = new File(path);
            String key = PARENTFILE + sellerId + "_" + activityId + "_" + System.currentTimeMillis() + ".csv.zip";
            File zipFile = ossAccessClient.zip(finalFile, path);
            OSSResponse rsp = ossAccessClient.put("wjbcrm", key, zipFile, DateUtils.nextDays(new Date(), 7));
            task.setTitle(task.getTitle());
            task.setData((rsp.getUrlOss() + "").replace("wjbcrm.oss-internal.aliyuncs.com","wjbcrm.oss.aliyuncs.com"));
            task.setDescription(task.getDescription() + "  key:" + rsp.getKeyVlaue());
            System.out.println(rsp.getUrlOss());
            task.setStatus("done");

        }catch(Exception e){
            task.setStatus("fail");
            task.setData(e.getMessage());
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void clean(SysSyntaskDo sysSyntaskDo) {

    }

    private String getEmailSendStatus(Integer status){
        String statusName="";
        switch(status){
            case 0: statusName="等待";break;
            case 1: statusName="发送成功";break;
            case 2: statusName="发送失败";break;
            case 3: statusName="发送失败";break;
            case 4: statusName="发送失败（邮箱地址有误）";break;
            case 5: statusName="发送失败（余额不足）";break;
            case 6: statusName="发送失败（运营商返回失败）";break;
            case 7: statusName="发送失败（黑名单）";break;
            case 110: statusName="发送内容为空";break;
            case 120: statusName="邮件内容不合法,有绝对超连接地址";break;
            case 130: statusName="webpower邮件服务器返回错误";break;
            case 200: statusName="发送失败（网聚宝黑名单）";break;
            default:
                statusName="时段内重复发送";
                break;
        }
        return statusName;
    }
}
