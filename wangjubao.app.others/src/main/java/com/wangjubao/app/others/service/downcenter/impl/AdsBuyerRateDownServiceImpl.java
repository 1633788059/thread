package com.wangjubao.app.others.service.downcenter.impl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.supercsv.io.CsvListWriter;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.model.SellerTraderateDo;
import com.wangjubao.dolphin.biz.model.TradeRateDo;
import com.wangjubao.dolphin.biz.service.TradeRateReportService;
import com.wangjubao.dolphin.biz.service.model.TradeRateBo;
import com.wangjubao.dolphin.common.util.ConfigurationUtils;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.date.DateUtils;

@Service("adsBuyerRateDownService")
public class AdsBuyerRateDownServiceImpl extends BuyerRateDownServiceImpl{
	
	protected static final String RATE_KEY_WORD_FILE = "rate.level.keyword_zh_CN.properties";
	protected static final String POSITIVE_RATE_KEYWORD = "positive.rate.keyword";
	protected static final String NEGATIVE_RATE_KEYWORD = "negative.rate.keyword";
	
	@Autowired
	private TradeRateReportService tradeRateReportService;

	@Override
	protected String[] getColumnName(){
		return new String[]{"商品ID","商家编码","商家SKU编码","订单编号","评价内容","是否好评","会员ID","会员等级","评价生效时间","付款时间","订单成功时间","卖家店铺","商品标题","省份","城市","区县","详细地址"};
	}
	
	@Override
	protected List<String[]> getRow(TradeRateBo tradeRateBo, SellerTraderateDo sellerRateDo) {
		List<String[]> rateList = new ArrayList<String[]>();			
	    Gson gson = new GsonBuilder().create();
		String itemId = sellerRateDo.getItemId();
		String tradeNo = sellerRateDo.getTid();
		String created = DateUtils.formatDate(new Date(sellerRateDo.getAdsCreated().longValue()));
		String payTime = "";
		if(sellerRateDo .getAdsPayTime() != null) {
			payTime = DateUtils.formatDate(new Date(sellerRateDo.getAdsPayTime().longValue()));
		} 
		String endTime = "";
		if(sellerRateDo.getAdsEndTime() != null) {
			endTime = DateUtils.formatDate(new Date(sellerRateDo.getAdsEndTime().longValue()));
		}
		String outerId = "";
		if(sellerRateDo.getOuterId() != null) {
			outerId = sellerRateDo.getOuterId();
		}
		String outerSkuId = "";
		if(sellerRateDo.getOuterSkuId() != null){
			outerSkuId = sellerRateDo.getOuterSkuId();
		}
		String rateNick = "";
		if(sellerRateDo.getRateNick() != null)
			rateNick = sellerRateDo.getRateNick();
		String buyerGrade = getBuyerGrade(sellerRateDo.getBuyerGrade());
		
		String province = "";
		if(sellerRateDo.getReceiverState() != null)
			province = sellerRateDo.getReceiverState();
		String city = "";
		if(sellerRateDo.getReceiverCity() != null)
			city = sellerRateDo.getReceiverCity();
		String district = "";
		if(sellerRateDo.getReceiverDistrict() != null)
			district = sellerRateDo.getReceiverDistrict();
		String address = "";
		if(sellerRateDo.getReceiverAddress() != null)
			address = sellerRateDo.getReceiverAddress();
		String rateJson = sellerRateDo.getRatejson();
		JsonArray rateArray = new JsonParser().parse(rateJson).getAsJsonArray();
		
		String ratedNick = "";
		String itemTitle = "";
		for (int r = 0; r < rateArray.size(); r++) {
//		if(rateArray.size() > 0) {
			String rateMapStr = rateArray.get(r).toString();
			if(rateMapStr.trim().length()==0)
				return rateList;
			
			TradeRateDo rateDo = gson.fromJson(rateMapStr, TradeRateDo.class);
			
			String content = rateDo.getContent();
			content = content.replace('\n', ' ').replace('\r', ' ');
			int rateLevel = 0;
			if("negative".equals(rateDo.getResult())){
				rateLevel = -1;
			}else{
				rateLevel = getRateLevel(content);
			}
			String rateLevelStr = "是";
			if(rateLevel < 0) {
				if("positive".equals(tradeRateBo.getRateResults()))
					continue;
				rateLevelStr = "否";
			}else if(rateLevel > 0){
				if("negative".equals(tradeRateBo.getRateResults()))
					continue;
			}else{
				rateLevelStr = "待确认";
			}

			List<String> columnList = new ArrayList<String>();
			if (itemId!=null) {
				columnList.add(itemId);
			}else {
				columnList.add("");
			}
			columnList.add(outerId);
			columnList.add("=\""+outerSkuId+"\"");
			columnList.add("=\""+tradeNo+"\"");			
			columnList.add(content);			
			columnList.add(rateLevelStr);
			columnList.add(rateNick);
			columnList.add(buyerGrade);		
			columnList.add(created);
			if(payTime != null && !payTime.isEmpty()) { 
				columnList.add(payTime);
			} else {
				columnList.add("");
			}	
			columnList.add(endTime);
			if(rateDo.getRatedNick() != null){
				ratedNick = rateDo.getRatedNick();		    
			}
			columnList.add(ratedNick);
			if(rateDo.getItemTitle() != null){
				itemTitle = rateDo.getItemTitle();
			}
			columnList.add(itemTitle);
			columnList.add(province);
			columnList.add(city);
			columnList.add(district);
			columnList.add(address);
			rateList.add(columnList.toArray(new String[0]));
		}
		
		return rateList;
	}
	
	@Override
	protected void retrieveOneDayRateAndWrite(TradeRateBo tradeRateBo, CsvListWriter csvWriter) throws IOException{
		logger.info("Start to download ADS buyer rate [{}]-({}~{})", new Object[]{tradeRateBo.getSellerId(), tradeRateBo.getBegin(), tradeRateBo.getEnd()});
		
		Integer totalResult = tradeRateReportService.countTradeRate(tradeRateBo);
		int rateCount = 0;
		if (totalResult != null)
			rateCount = totalResult.intValue();
		logger.info("Trade rate count {}", totalResult);

        if(rateCount >= 10000){
        	Date startTime = DateUtils.parseDay(tradeRateBo.getBegin());
        	Date endTime = DateUtils.parseDay(tradeRateBo.getEnd());
        	Integer diffGap = DateUtils.diffSecond(endTime, startTime);
        	int step = diffGap.intValue()/2;
        	if(step > 0){
        	    Date midTime = DateUtils.add(startTime, Calendar.SECOND, step);
        	    TradeRateBo tmpTradeRateBo = new TradeRateBo();
        	    tmpTradeRateBo.setSellerId(tradeRateBo.getSellerId());
        	    tmpTradeRateBo.setBegin(tradeRateBo.getBegin());
        	    tmpTradeRateBo.setEnd(DateUtils.formatDate(midTime));
        	    retrieveOneDayRateAndWrite(tmpTradeRateBo, csvWriter);
        	    midTime = DateUtils.add(midTime, Calendar.SECOND, 1);
        	    tmpTradeRateBo.setBegin(DateUtils.formatDate(midTime));
        	    tmpTradeRateBo.setEnd(tradeRateBo.getEnd());
        	    retrieveOneDayRateAndWrite(tmpTradeRateBo, csvWriter);
        	}
        }else if(rateCount > 0){
			List<SellerTraderateDo> rateList = tradeRateReportService.listAllTradeRate(tradeRateBo);
			for (SellerTraderateDo sellerRateDo : rateList) {
				List<String[]> rowList = getRow(tradeRateBo, sellerRateDo);
				for(String[] row : rowList)
					csvWriter.write(row);	
			}
        }
	}
	
	//0:店铺客户，1：普通会员，2：高级会员，3：VIP会员， 4：至尊VIP会员
	protected String getBuyerGrade(String gradeValue){
		if(gradeValue == null)
			return "";
		else if("0".equals(gradeValue))
            return "店铺客户";
		else if("1".equals(gradeValue))
            return "普通会员";
		else if("2".equals(gradeValue))
            return "高级会员";
		else if("3".equals(gradeValue))
            return "VIP会员";
		else if("4".equals(gradeValue))
            return "至尊VIP会员";
		return "";
	}
	
	protected int getRateLevel(String content){
		if(content.trim().length()==0 || content.trim().equals("好评！") || content.trim().equals("好评") || content.trim().equals("好"))
			return 1;
		String[] posKeywords = ConfigurationUtils.getStringArray(RATE_KEY_WORD_FILE, POSITIVE_RATE_KEYWORD);
		String[] negKeywords = ConfigurationUtils.getStringArray(RATE_KEY_WORD_FILE, NEGATIVE_RATE_KEYWORD);
		for(String keyword : negKeywords){
			if(content.contains(keyword))
				return -1;
		}
		for(String keyword : posKeywords){
			if(content.contains(keyword))
				return 1;
		}
		return 0;
	}
	
	public static void main(String[] args){
		String content = "宝贝质量不错，可是不适合我穿，标签已经剪掉，没有穿过，不知道能不能退换！318的价格有点贵，一次分别在3家买了3件，其他两件质量也都还不错，也是品牌，只有100多！";
		AdsBuyerRateDownServiceImpl service = new AdsBuyerRateDownServiceImpl();
		int result = service.getRateLevel(content);
		System.out.println("Rate level:"+result);
	}
}
