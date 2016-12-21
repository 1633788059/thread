package com.wangjubao.app.others.service.downcenter.impl;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wangjubao.core.dao.ICommonDao;
import com.wangjubao.core.domain.marketing.ActivityInfo;
import com.wangjubao.core.domain.marketing.ActivitySendContent;
import com.wangjubao.core.service.marketing.IActivityInfoService;
import com.wangjubao.dolphin.biz.dao.sequence.SequenceSupport;
import com.wangjubao.dolphin.biz.model.SysSyntaskDo;
import com.wangjubao.dolphin.biz.oss.OSSAccessClient;
import com.wangjubao.dolphin.biz.service.impl.SellerServiceImpl;
import com.wangjubao.dolphin.common.util.StrUtils;
import com.wangjubao.dolphin.common.util.date.DateUtils;

@Service("marketingUploadService")
public class MarketingUploadServiceImpl extends AbstractDownCenterServiceImpl {

static Logger logger = Logger.getLogger("downcenter");
	
private static final String PARENTFILE="marketingUpload/";



	OSSAccessClient ossAccessClient = new OSSAccessClient();
	
	@Autowired
	private SellerServiceImpl sellerService;
	
	@Autowired
	private SequenceSupport sequenceSupport;
	
	@Autowired
	private ICommonDao commonDao;
	
	@Autowired
	private IActivityInfoService activityInfoService;
	
	private String ossFileBucket = "wjbcrmmarketfile";
	
	@Override
	public void init(SysSyntaskDo task) {
		ActivityInfo actInfo = new ActivityInfo();
		actInfo.setSellerId(task.getSellerId());
		String jsonStr = task.getParam();
		JsonParser parser = new JsonParser();
		JsonObject json = parser.parse(jsonStr).getAsJsonObject();
		long activityId = json.get("activityId").getAsLong();
		actInfo.setActivityId(Long.valueOf(activityId));
		
		ActivityInfo dbActInfo = activityInfoService.queryActivityInfoById(actInfo);
		if(dbActInfo != null && dbActInfo.isNotDeleted() && dbActInfo.getCurrentStatus().equals(ActivityInfo.CURRENT_STATUS_6)){
			actInfo.setCurrentStatus(ActivityInfo.CURRENT_STATUS_7);
			activityInfoService.updateActivityInfoById(actInfo);
			
			task.setStatus("fail");
		}else{
			task.setStatus("done");
		}
		sellerService.updateSysSyntaskDo(task);
	}

	@Override
	public void job(SysSyntaskDo task) {

//		task.setOwnerId(serverIp);
		task.setStatus("doing");
		sellerService.updateSysSyntaskDo(task);
		
		String jsonStr = task.getParam();
		logger.debug("##>>Marketing upload parameters>>>" + jsonStr);
		JsonParser parser = new JsonParser();
		JsonObject json = parser.parse(jsonStr).getAsJsonObject();
		long activityId = json.get("activityId").getAsLong();
		String fileNameList = task.getData();
		String[] keyList = fileNameList.split(",");
		
		ActivityInfo activityInfo = new ActivityInfo();
		activityInfo.setSellerId(task.getSellerId());
		activityInfo.setActivityId(Long.valueOf(activityId));
		ActivityInfo dbActInfo = activityInfoService.queryActivityInfoById(activityInfo);
		if(dbActInfo == null || dbActInfo.isDeleted()   				 //防止任务运行时，activity已被删除或者已经被update
				|| dbActInfo.getCurrentStatus() == ActivityInfo.CURRENT_STATUS_0
				|| dbActInfo.getCurrentStatus() == ActivityInfo.CURRENT_STATUS_7){
			/*
			task.setStatus("done");
			sellerService.updateSysSyntaskDo(task);
			*/
			return;
		}
		
		 ActivitySendContent originalContent = new ActivitySendContent();
		 originalContent.setActivityId(activityInfo.getActivityId());
		 originalContent.setSellerId(activityInfo.getSellerId());
	     commonDao.deleteActivitySendContent(originalContent);
		
		List<String> urlList = new ArrayList<String>(keyList.length);
		InputStream in = null;
		try{
			for(String key : keyList){
				in = ossAccessClient.openOssObjectStream(ossFileBucket,key);
				InputStreamReader bReader = new InputStreamReader(in);
				CsvListReader csvReader = new CsvListReader(bReader, CsvPreference.STANDARD_PREFERENCE);				
						
				ActivitySendContent activitySendContent = null;
				List<ActivitySendContent> contentList = new ArrayList<ActivitySendContent>();

				List<String> dataList = null;
				int step = 500;
				do{
					dataList = csvReader.read();
					if(dataList==null || dataList.size()==0 || dataList.get(0).trim().isEmpty())
						continue;
					String dataItem = dataList.get(0).toString().trim();
					if((task.getCode().equals(SysSyntaskDo.TaskCodeType.MARKET_MOBILE_UPLOAD.name()) && StrUtils.isMobile(dataItem))
							|| (task.getCode().equals(SysSyntaskDo.TaskCodeType.MARKET_EMAIL_UPLOAD.name()) && StrUtils.isEmail(dataItem))){
						activitySendContent = new ActivitySendContent();
						activitySendContent.setActivityId(Long.valueOf(activityId));
						activitySendContent.setSellerId(task.getSellerId());
						activitySendContent.setContent(dataItem);
						activitySendContent.setId(sequenceSupport.nextSuperId());
						contentList.add(activitySendContent);
					} 
				    if(contentList.size() >= step){
				    	commonDao.batchInsert("insertActivitySendContent", contentList);
				    	contentList.clear();
				    }
				}while(dataList != null);
				if(contentList.size() > 0){
					commonDao.batchInsert("insertActivitySendContent", contentList);
				}

				try{
				    in.close();
				}catch(Exception e){					
				}finally{
					in = null;
				}  					
				java.net.URL ossUrl = ossAccessClient.generateOssObjectUrl(ossFileBucket,key,DateUtils.nextDays(new Date(), 7) );
				if(ossUrl != null)
					urlList.add(ossUrl.toString());
			}
			//Re-Check, 防止导入过程中用户重新导入
			SysSyntaskDo taskDo = sellerService.loadSysSyntaskDo(task);
			if(taskDo!=null && taskDo.getDeletedFlag().intValue()!=1){
				StringBuilder urlBuilder = new StringBuilder();
				for(String url : urlList)
					urlBuilder.append(url).append(',');
				urlBuilder.deleteCharAt(urlBuilder.length()-1);
				JsonObject jsonDetail = new JsonParser().parse(dbActInfo.getActivityDetailgsontStr()).getAsJsonObject();
				jsonDetail.addProperty("ossFileUrl", urlBuilder.toString());
//				jsonDetail.addProperty("smsTemplateId", jsonDetail.remove("smsTemplateId").toString());					
				activityInfo.setActivityDetailgsontStr(jsonDetail.toString());
				/*
				JsonActivityDetail jsonDetail = dbActInfo.activityDetailJsonStr2Object();
				jsonDetail.setOssFileUrl(urlBuilder.toString());
				activityInfo.setActivityDetail(jsonDetail);
				*/
				activityInfo.setCurrentStatus(ActivityInfo.CURRENT_STATUS_0);
				activityInfo.setUpdateTime(new Date());
				this.activityInfoService.updateActivityInfoById(activityInfo);
			}
			
			task.setStatus("done");
		}catch(Exception e){
			logger.error("Error found when read mobile info from file",  e);
			activityInfo.setCurrentStatus(ActivityInfo.CURRENT_STATUS_7);
			this.activityInfoService.updateActivityInfoById(activityInfo);
			
			task.setStatus("fail");
		}finally{
			if(in != null){
				try{
					in.close();
				}catch(Exception e){					
			    }finally{
					in = null;
				}
			}
			sellerService.updateSysSyntaskDo(task);
		}		
	
	}

	@Override
	public void clean(SysSyntaskDo sysSyntaskDo) {
		// TODO Auto-generated method stub

	}

}
