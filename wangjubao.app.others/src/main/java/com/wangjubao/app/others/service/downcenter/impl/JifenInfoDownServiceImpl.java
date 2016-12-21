package com.wangjubao.app.others.service.downcenter.impl;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
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
import com.wangjubao.dolphin.biz.dao.JifenAccountDao;
import com.wangjubao.dolphin.biz.model.JifenAccountDo;
import com.wangjubao.dolphin.biz.model.SellerTraderateDo;
import com.wangjubao.dolphin.biz.model.SysSyntaskDo;
import com.wangjubao.dolphin.biz.model.TradeRateDo;
import com.wangjubao.dolphin.biz.oss.OSSAccessClient;
import com.wangjubao.dolphin.biz.oss.OSSResponse;
import com.wangjubao.dolphin.biz.service.SellerService;
import com.wangjubao.dolphin.biz.service.impl.SellerServiceImpl;
import com.wangjubao.dolphin.biz.service.model.TradeRateBo;
import com.wangjubao.dolphin.common.util.JodaTime;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.date.DateUtils;

@Service("jifenInfoDownService")
public class JifenInfoDownServiceImpl extends AbstractDownCenterServiceImpl {

	static Logger logger = LoggerFactory.getLogger("downcenter");
	
	private static final String PARENTFILE="jifenInfoDown/";
	
	OSSAccessClient ossAccessClient = new OSSAccessClient();
	
	@Autowired
	private SellerService sellerService;
	@Autowired
	private JifenAccountDao jifenAccountDao;
	
	@Override
	public void init(SysSyntaskDo sysSyntaskDo) {
		sysSyntaskDo.setStatus("init");
		sellerService.updateSysSyntaskDo(sysSyntaskDo);
	}

	@Override
	public void job(SysSyntaskDo task) {
		
		if(task.getDeletedFlag().intValue()==1){
			logger.info("Task has been removed");
			return;
		}
		task.setStatus("doing");
		sellerService.updateSysSyntaskDo(task);
		logger.info("Start to download jifen info "+task.getSellerId()+"【"+task.getTitle()+"】");
		
		String jsonStr = task.getParam();
		JsonParser parser = new JsonParser();
		JsonObject json = parser.parse(jsonStr).getAsJsonObject();
		
		String beginMoney = null;
		String endMoney = null;
		String beginPoint = null;
		String endPoint = null;
		String isBind = null;
		Long buyerLevel = null;
		
		JsonElement element = json.get("isBind");
		if (element != null)
			isBind = element.getAsString();
		element = json.get("beginMoney");
		if (element != null)
			beginMoney = element.getAsString();
		element = json.get("endMoney");
		if (element != null)
			endMoney = element.getAsString();
		element = json.get("beginPoint");
		if (element != null)
			beginPoint = element.getAsString();
		element = json.get("endPoint");
		if (element != null)
			endPoint = element.getAsString();
		element = json.get("buyerLevel");
		if (element != null)
			buyerLevel = Long.valueOf(element.getAsString());
		
		
		String now = JodaTime.formatDate(new Date(), "yyyyMMddHHmmss");
		String fileName = new StringBuilder("JifenInfo_")
				.append(task.getSellerId())
				.append('_').append(task.getId())
				.append('_').append(now)
				.append(".csv").toString();
		String path = "conf/"+fileName;
		
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
			csvWriter.writeHeader(getColumnName());
			
			
		
			Integer totalResult = jifenAccountDao.listForDownloadCount(task.getSellerId(),isBind,buyerLevel,beginMoney,endMoney,beginPoint,endPoint);
			int jifenInfoCount = 0;
			if (totalResult != null)
				jifenInfoCount = totalResult.intValue();
			PageQuery pageQuery = new PageQuery(1, 50);
			
			while (pageQuery.getStartIndex() < jifenInfoCount) {
				PageList<JifenAccountDo> jifenAccountDoList = jifenAccountDao.listForDownload(task.getSellerId(),isBind,buyerLevel,
		        		beginMoney,endMoney,beginPoint,endPoint,pageQuery,jifenInfoCount);
				pageQuery.increasePageNum();
				for (JifenAccountDo jifenAccountDo : jifenAccountDoList) {
					//String[]{"手机号","会员昵称","消费金额","已用积分","可用积分","积分状态","会员等级"};
					
					List<String> columnList = new ArrayList<String>();
					if(jifenAccountDo.getBuyerMobile()!=null){
						columnList.add(jifenAccountDo.getBuyerMobile());
					}else{
						columnList.add("");
					}
					if(jifenAccountDo.getBuyerNick()!=null){
						columnList.add(jifenAccountDo.getBuyerNick());
					}else{
						columnList.add("");
					}
					if(jifenAccountDo.getTotalAmount()!=null){
						columnList.add(jifenAccountDo.getTotalAmount().toString());
					}else{
						columnList.add("");
					}
					if(jifenAccountDo.getUsed()!=null){
						columnList.add(jifenAccountDo.getUsed().toString());
					}else{
						columnList.add("");
					}
					if(jifenAccountDo.getBalance()!=null){
						columnList.add(jifenAccountDo.getBalance().toString());
					}else{
						columnList.add("");
					}
					if(jifenAccountDo.getStatus()!=null){
						if(jifenAccountDo.getStatus()==0){
							columnList.add("正常");
						}else if(jifenAccountDo.getStatus()==1){
							columnList.add("锁定");
						}else{
							columnList.add("其他");
						}
					}else{
						columnList.add("");
					}
					if(jifenAccountDo.getBuyerLevel()!=null){
						columnList.add(jifenAccountDo.getBuyerLevel().toString());
					}else{
						columnList.add("");
					}
					
					
					List<String[]> rowList = new ArrayList<String[]>();	
					rowList.add(columnList.toArray(new String[0]));
					for(String[] row : rowList)
						csvWriter.write(row);	
				}
			}
			
			String key = new StringBuilder(PARENTFILE)
					.append("JifenInfo_")
					.append(task.getSellerId()).append("_")
					.append(task.getId()).append('_')
					.append(now).append(".csv.zip")
					.toString();
			if(csvWriter != null){
				csvWriter.close();
			}
			
			File zipFile = ossAccessClient.zip(file, fileName);
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

	
	protected String[] getColumnName() {
		return new String[]{"手机号","会员昵称","消费金额","已用积分","可用积分","积分状态","会员等级"};
	}

}
