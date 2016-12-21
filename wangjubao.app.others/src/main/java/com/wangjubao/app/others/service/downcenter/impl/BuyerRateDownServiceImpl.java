package com.wangjubao.app.others.service.downcenter.impl;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.quote.AlwaysQuoteMode;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.model.SellerTraderateDo;
import com.wangjubao.dolphin.biz.model.SysSyntaskDo;
import com.wangjubao.dolphin.biz.model.TradeRateDo;
import com.wangjubao.dolphin.biz.oss.OSSAccessClient;
import com.wangjubao.dolphin.biz.oss.OSSResponse;
import com.wangjubao.dolphin.biz.service.SellerService;
import com.wangjubao.dolphin.biz.service.SellerTraderateService;
import com.wangjubao.dolphin.biz.service.model.TradeRateBo;
import com.wangjubao.dolphin.common.util.JodaTime;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.StrUtils;
import com.wangjubao.dolphin.common.util.date.DateUtils;

@Service("buyerRateDownService")
public class BuyerRateDownServiceImpl extends AbstractDownCenterServiceImpl {

	static Logger logger = LoggerFactory.getLogger("downcenter");
	
	private static final String PARENTFILE="buyerRateDown/";
	
	OSSAccessClient ossAccessClient = new OSSAccessClient();
	@Autowired
	private SellerService sellerService;
	
	@Autowired
	private SellerTraderateService sellerTraderateService;
	
	@Override
	public void init(SysSyntaskDo sysSyntaskDo) {
		sysSyntaskDo.setStatus("init");
		sellerService.updateSysSyntaskDo(sysSyntaskDo);
	}

	@Override
	public void job(SysSyntaskDo task) {
		GsonBuilder builder = new GsonBuilder();
		Gson gson = builder.create();

		if(task.getDeletedFlag().intValue() == 1) {
			logger.info("Task has been removed");
			return;
		}
		task.setStatus("doing");
		sellerService.updateSysSyntaskDo(task);
		logger.info("Start to download trade rate "+task.getSellerId()+"【" +task.getTitle()+"】");

		String jsonStr = task.getParam();
		JsonParser parser = new JsonParser();
		JsonObject json = parser.parse(jsonStr).getAsJsonObject();
		String rateResults = null;
		String rateTimeBegin = null;
		String rateTimeEnd = null;
		String tradeTimeBegin = null;
		String tradeTimeEnd = null;
		String sourceItemId = null;
		String keyword = null;
//		JsonElement element = json.get("rateResults");
//		if (element != null)
//			rateResults = element.getAsString();
		JsonElement element = json.get("rateTimeBegin");
		if (element != null)
			rateTimeBegin = element.getAsString();
		element = json.get("rateTimeEnd");
		if (element != null)
			rateTimeEnd = element.getAsString();
		element = json.get("tradeTimeBegin");
		if (element != null)
			tradeTimeBegin = element.getAsString();
		element = json.get("tradeTimeEnd");
		if (element != null)
			tradeTimeEnd = element.getAsString();
//		element = json.get("fields");
		element = json.get("rateResults");
		if(element != null)
			rateResults = element.getAsString();
		element = json.get("sourceItemId");
		if(element != null)
			sourceItemId = element.getAsString();
		element = json.get("keywords");
		if(element != null)
			keyword = element.getAsString();

		boolean byRateTime = false;
		boolean byTradeTime = false;
		Date beginDate = null;
		Date endDate = null;
		if (tradeTimeBegin != null) {
			byTradeTime = true;
			beginDate = DateUtils.parseDay(tradeTimeBegin);
			endDate = DateUtils.parseDay(tradeTimeEnd);
		} else {
			byRateTime = true;
			beginDate = DateUtils.parseDay(rateTimeBegin);
			endDate = DateUtils.parseDay(rateTimeEnd);
		}
		if (beginDate == null)
			beginDate = DateUtils.startOfDate(DateUtils.now());
		if (endDate == null)
			endDate = DateUtils.endOfDate(DateUtils.now());

		String now = JodaTime.formatDate(new Date(), "yyyyMMddHHmmss");
		String fileName = new StringBuilder("Rate_")
				.append(task.getSellerId())
				.append('_').append(task.getId())
				.append('_').append(now)
				.append(".csv").toString();

		String path = "conf/"+fileName;
		
//		path = path.replaceAll("ExportReport.csv", fileName);

		final File file = new File(path);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e1) {
				logger.error(e1.getMessage());
			}
		}
		
		CsvListWriter csvWriter = null;
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(file, "GBK");
			CsvPreference.Builder preferBuilder = new CsvPreference.Builder(CsvPreference.EXCEL_PREFERENCE).useQuoteMode(new AlwaysQuoteMode());
			CsvPreference csvPreference = preferBuilder.build();
			csvWriter = new CsvListWriter(pw, csvPreference);
//			logger.info("Start to write header.");
			csvWriter.writeHeader(getColumnName());
//			logger.info("csv line count" + csvWriter.getLineNumber());
//			logger.info("file header count" + file.length());
//			logger.info("begin date:" + beginDate);
//			logger.info("end date:" + endDate);
			
			while (beginDate.before(endDate)) {
//				logger.info("Start to compare date.");
				Date endTime = DateUtils.endOfDate(beginDate);
				if (endTime.after(endDate))
					endTime = endDate;
				TradeRateBo tradeRateBo = new TradeRateBo();
				tradeRateBo.setSellerId(task.getSellerId());
				if (byRateTime) {
					tradeRateBo.setBegin(DateUtils.formatDate(beginDate));
					tradeRateBo.setEnd(DateUtils.formatDate(endTime));
				} else {
					tradeRateBo.setTradeBegin(DateUtils.formatDate(beginDate));
					tradeRateBo.setTradeEnd(DateUtils.formatDate(endTime));
				}
				tradeRateBo.setRateResults(rateResults);
				tradeRateBo.setSourceItemId(sourceItemId);
				
				if(StrUtils.isNotEmpty(keyword)) {
					String[] rateKeywords = keyword.split(" ");
					List<String> keywordList = Arrays.asList(rateKeywords);
					for(Iterator<String> it= keywordList.iterator(); it.hasNext();){
						if(it.next().isEmpty())
							it.remove();
					}
					if(keywordList.size() > 0)
	                    tradeRateBo.setRateKeywords(keywordList.toArray(new String[0]));
				}
//				logger.info("Retrieve trade rate {}-{}",
//						DateUtils.formatDate(beginDate),
//						DateUtils.formatDate(endTime));
				
				retrieveOneDayRateAndWrite(tradeRateBo, csvWriter);
/*
				Integer totalResult = sellerTraderateService
						.countAllTradeRate(tradeRateBo);
				int rateCount = 0;
				if (totalResult != null)
					rateCount = totalResult.intValue();

//				logger.info("PageIndex = {}", pageQuery.getStartIndex());
//				logger.info("Rate Count = {}", rateCount);
				PageQuery pageQuery = new PageQuery(1, 50);
				while (pageQuery.getStartIndex() < rateCount) {
					PageList<SellerTraderateDo> rateList = sellerTraderateService
							.listAllTradeRateByPage(tradeRateBo, pageQuery);
					pageQuery.increasePageNum();
//					logger.info("PageIndex = {}", pageQuery.getStartIndex());
					
					SimpleDateFormat simpleFormat = new SimpleDateFormat(
							"yyyy-MM-dd HH:mm:ss");
					for (SellerTraderateDo sellerRateDo : rateList) {
						List<String[]> rowList = getRow(sellerRateDo);
						for(String[] row : rowList)
							csvWriter.write(Arrays.asList(row));	
					}
				}
*/
				beginDate = DateUtils.add(endTime, Calendar.SECOND, 1);
			}
//			logger.info("Start to write file : " + fileName);
			String key = new StringBuilder(PARENTFILE).append("Rate_")
					.append(task.getSellerId()).append("_")
					.append(task.getId()).append('_')
					.append(now).append(".csv.zip")
					.toString();
			if(csvWriter != null) {
				csvWriter.close();
			}
			
			File zipFile = ossAccessClient.zip(file, fileName);
//			logger.info("file length: " + file.length());
//			logger.info("Start to write file: " + zipFile);
//			logger.info("Start to write key: " + key);
			OSSResponse rsp = ossAccessClient.put("wjbcrm",key, zipFile,DateUtils.nextDays(new Date(), 7));
			if(rsp.getUrlOss()==null){
				logger.error("OSS错误信息=========="+rsp.getErrorMessage());
				logger.error(rsp.getMsg());
			}
			file.delete();
			zipFile.delete();
			task.setTitle(task.getTitle());
			task.setData((rsp.getUrlOss() + "").replace("wjbcrm.oss-internal.aliyuncs.com","wjbcrm.oss.aliyuncs.com"));
			task.setOwnerId(rsp.getKeyVlaue());
			task.setDescription(task.getDescription() + "  key:" + rsp.getKeyVlaue());
			task.setStatus("done");
		} catch (Exception e) {
			task.setStatus("fail");
			task.setData(e.getMessage());
			logger.error(e.getMessage(), e);
		} finally {
			if (null != pw) {
				pw.flush();
				pw.close();
			}
		}

		try {
			if (task.getTitle().length() > 200) {
				task.setTitle(task.getTitle().substring(0, 200));
			}
			sellerService.updateSysSyntaskDo(task);
		} catch (Exception ex) {
			task = sellerService.loadSysSyntaskDo(task);
			task.setData(ex.getMessage());
		}
	

	}

	@Override
	public void clean(SysSyntaskDo task) {
		try {
			task.setDeletedFlag(1);
			OSSResponse rsp = null;
			for(String key : task.getData().split(",")){
				if(key.indexOf(':') > 0)
					key = key.substring(0, key.indexOf(':'));
			    rsp = ossAccessClient.delete(key, "wjbcrm");
			    if(!rsp.isSuccess())
			    	break;
			}
			if(rsp.isSuccess())
			    sellerService.updateSysSyntaskDo(task);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	protected String[] getColumnName(){
		return new String[]{"商品id","订单编号","评价内容","评价解释","买家id","付款时间","评价生效时间","卖家店铺","商品标题"};
	}
	
	protected List<String[]> getRow(TradeRateBo tradeRateBo, SellerTraderateDo sellerRateDo) {	
	    List<String[]> rateList = new ArrayList<String[]>();	
	
	    Gson gson = new GsonBuilder().create();
		String itemId = sellerRateDo.getItemId();
		if(StrUtils.isEmpty(itemId)){
			itemId = sellerRateDo.getNumIid();
		}
		String tradeNo = sellerRateDo.getTid();
		String created = DateUtils.formatDate(sellerRateDo.getCreated());
		String payTime = "";
		if(sellerRateDo .getPayTime() != null) {
			payTime = DateUtils.formatDate(sellerRateDo.getPayTime());
		} 
		String rateNick = sellerRateDo.getRateNick();
		String rateJson = sellerRateDo.getRatejson();
		JsonArray rateArray = new JsonParser().parse(rateJson)
				.getAsJsonArray();
		for (int r = 0; r < rateArray.size(); r++) {
			String rateMapStr = rateArray.get(r).toString();
			TradeRateDo rateDo = gson.fromJson(rateMapStr, TradeRateDo.class);

			List<String> columnList = new ArrayList<String>();
			if (itemId!=null) {
				columnList.add(itemId);
			}else {
				columnList.add("");
			}
			columnList.add("=\""+tradeNo+"\"");
			columnList.add(rateDo.getContent());
			if(rateDo.getReply() != null) {
				columnList.add(rateDo.getReply());
			} else {
				columnList.add("");
			}
			columnList.add(rateNick);
			if(payTime != null && !payTime.isEmpty()) { 
				columnList.add(payTime);
			} else {
				columnList.add("");
			}
			columnList.add(created);
			columnList.add(rateDo.getRatedNick());
			columnList.add(rateDo.getItemTitle());
			
			rateList.add(columnList.toArray(new String[0]));
		}
		
		return rateList;
	}
	
	
	protected void retrieveOneDayRateAndWrite(TradeRateBo tradeRateBo, CsvListWriter csvWriter) throws IOException{
		Integer totalResult = sellerTraderateService
				.countAllTradeRate(tradeRateBo);
		int rateCount = 0;
		if (totalResult != null)
			rateCount = totalResult.intValue();

		PageQuery pageQuery = new PageQuery(1, 50);
		while (pageQuery.getStartIndex() < rateCount) {
			PageList<SellerTraderateDo> rateList = sellerTraderateService
					.listAllTradeRateByPage(tradeRateBo, pageQuery);
			pageQuery.increasePageNum();

			for (SellerTraderateDo sellerRateDo : rateList) {
				List<String[]> rowList = getRow(tradeRateBo, sellerRateDo);
				for(String[] row : rowList)
					csvWriter.write(row);	
			}
		}
	}

}
