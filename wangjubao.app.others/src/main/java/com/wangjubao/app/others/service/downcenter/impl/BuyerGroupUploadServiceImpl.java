package com.wangjubao.app.others.service.downcenter.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wangjubao.app.others.downcenter.main.DownCenterMain;
import com.wangjubao.core.util.BuyerSearchContext;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dao.AreaDao;
import com.wangjubao.dolphin.biz.dao.BuyerDao;
import com.wangjubao.dolphin.biz.dao.OrderDao;
import com.wangjubao.dolphin.biz.dao.UserDynamicGroupClassDao;
import com.wangjubao.dolphin.biz.dao.UserDynamicGroupDao;
import com.wangjubao.dolphin.biz.model.AreaDo;
import com.wangjubao.dolphin.biz.model.SysSyntaskDo;
import com.wangjubao.dolphin.biz.model.UserDynamicGroupDo;
import com.wangjubao.dolphin.biz.oss.OSSAccessClient;
import com.wangjubao.dolphin.biz.oss.OSSResponse;
import com.wangjubao.dolphin.biz.service.BuyerImportService;
import com.wangjubao.dolphin.biz.service.DolphinBuyerSearcher;
import com.wangjubao.dolphin.biz.service.SellerService;
import com.wangjubao.dolphin.biz.service.UserDynamicGroupService;
import com.wangjubao.dolphin.common.util.JodaTime;
import com.wangjubao.dolphin.common.util.StrUtils;
import com.wangjubao.dolphin.common.util.date.DateUtils;

@Service("buyerGroupUploadService")
public class BuyerGroupUploadServiceImpl extends AbstractDownCenterServiceImpl {

	static Logger logger = Logger.getLogger("downcenter");
	
	private static final String PARENTFILE="buyerGroupUpload/";
	
	OSSAccessClient ossAccessClient = new OSSAccessClient();
	@Autowired
	private SellerService sellerService;
	@Qualifier("dolphinBuyerDao")
	@Autowired
	private BuyerDao buyerDao;
	@Autowired
	@Qualifier("orderDao")
	private OrderDao orderDao;
	@Autowired
	private AreaDao areaDao;

	@Autowired
	UserDynamicGroupClassDao userDynamicGroupClassDao;

	@Autowired
	UserDynamicGroupDao userDynamicGroupDao;
	
	@Autowired
	private UserDynamicGroupService       userDynamicGroupService;

	@Autowired
	private BuyerImportService buyerImportService;
	
	static DolphinBuyerSearcher dolphinBuyerSearcher =null;
	
	private static String path=""; 
	
	private static String serverIp="0.0.0.0";
	
  private DolphinBuyerSearcher getDolphinBuyerSearcher()
    {
        if (dolphinBuyerSearcher == null)
        {
            dolphinBuyerSearcher = (DolphinBuyerSearcher) BuyerSearchContext.getInstance().getBean("dolphinBuyerSearcher");
        }
        return dolphinBuyerSearcher;
    }
	
	@Override
	public void init(SysSyntaskDo task) {
		task.setStatus("fail");			
		sellerService.updateSysSyntaskDo(task);
	}

	@Override
	public void job(SysSyntaskDo task) {
		try {
			InetAddress addr = InetAddress.getLocalHost();
			serverIp=addr.getHostAddress().toString(); //获取本机ip 
			
		} catch (Exception e) {
			
		}
		task.setOwnerId(serverIp);
		task.setStatus("doing");
		sellerService.updateSysSyntaskDo(task);
		
		String jsonStr = task.getParam();
		logger.debug("##>>Buyer group upload parameters>>>" + jsonStr);
		JsonParser parser = new JsonParser();
		JsonObject json = parser.parse(jsonStr).getAsJsonObject();
		JsonElement groupIdObject = json.get("groupId");
		String groupName = json.get("groupName").getAsString();
		long groupType = json.get("groupType").getAsLong();
		int userId = json.get("userId").getAsInt();
		String fileNameList = task.getData();
		
		try {
            UserDynamicGroupDo userDynamicGroup = new UserDynamicGroupDo();
            Long groupId = null;
            if(groupIdObject != null)
            	groupId = Long.valueOf(groupIdObject.getAsLong());
            else
            	groupId = userDynamicGroupService.SequenceSupport();

            userDynamicGroup.setGroupId(groupId);
            userDynamicGroup.setSellerId(task.getSellerId());
            userDynamicGroup.setUserId(Long.valueOf(userId));
                
			boolean numOfResult = importBuyer2DB(task.getCode(), fileNameList.split(","), userDynamicGroup, groupIdObject==null);
			if(numOfResult){
				userDynamicGroup.setGroupName(groupName);
                userDynamicGroup.setStatus(0);
                userDynamicGroup.setUpdateUserId(Integer.valueOf(userId));	                
                userDynamicGroup.setFilterType(2);
                userDynamicGroup.setGroupType(Long.valueOf(groupType));
                userDynamicGroup.setFocusGroupStatus(0);	                
                userDynamicGroup.setSourceFrom(1);
//                userDynamicGroup.setMemos("");  //set oss file url if import group
                java.util.Date cData = new java.util.Date();
                userDynamicGroup.setGmtModified(cData);
                userDynamicGroup.setUpdateTime(cData);
                if (groupIdObject == null){
	                userDynamicGroup.setGmtCreate(cData);	                
	                userDynamicGroup.setCreateTime(cData);	                
	                userDynamicGroup.setCreateUserId(Integer.valueOf(userId));
                    this.userDynamicGroupService.create(userDynamicGroup);
                }else{
                	userDynamicGroupService.update(userDynamicGroup);
                }
				task.setStatus("done");
			}else{
				task.setStatus("fail");
			}				
		} catch (Exception e) {				
			task.setTitle(task.getTitle()+" < 操作失败 >");
			task.setStatus("fail");
//			task.setData(e.getMessage());
			logger.error("Fail to finish dynamic group operaiton", e);
		}finally{
			sellerService.updateSysSyntaskDo(task);
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
	
	private boolean importBuyer2DB(String importType, String[] fileNameList, UserDynamicGroupDo userDynamicGroup, boolean isCreate){
		if(!isCreate){
			buyerImportService.deleteByGroupId(userDynamicGroup.getGroupId(), userDynamicGroup.getSellerId());
			userDynamicGroup.setMemberNum(Long.valueOf(0));
			userDynamicGroup.setMemberMobileNum(Long.valueOf(0));
			userDynamicGroup.setMemberEmailNum(Long.valueOf(0));
			userDynamicGroupService.update(userDynamicGroup);
		}
		List<String> urlList = new ArrayList<String>(fileNameList.length);
		InputStream in = null;
		try{
			for(String fileName : fileNameList){
				String file = fileName.substring(0, fileName.indexOf(':'));
				String charset = fileName.substring(fileName.indexOf(':')+1);
				in = ossAccessClient.openOssObjectStream("wjbcrm",file );
				InputStreamReader bReader = new InputStreamReader(in, charset);
				CsvListReader csvReader = new CsvListReader(bReader, CsvPreference.STANDARD_PREFERENCE);				
				
				List<String> dataList = null;
				int step = 10000;
				List<String> buyerNickList = new ArrayList<String>(step);
				do{
					dataList = csvReader.read();				
					if(dataList==null || dataList.size()==0 || StrUtils.isEmpty(dataList.get(0)))				
						continue;
						
					String nickName = dataList.get(0).toString();
					buyerNickList.add(nickName);					
	                if(buyerNickList.size() >= step){
						boolean subResult =  false;
						if(SysSyntaskDo.TaskCodeType.BUYER_GROUP_MOBILE_UPLOAD.toString().equals(importType)){
							subResult = userDynamicGroupService.importBuyersByMobile(userDynamicGroup, buyerNickList.toArray(new String[0]));
						}else{
							subResult = userDynamicGroupService.importBuyers(userDynamicGroup, buyerNickList.toArray(new String[0]));
						}
						if(!subResult)
							 return subResult;
						 buyerNickList.clear();
	                }
				}while(dataList != null);
				if(buyerNickList.size() > 0){
					boolean subResult =  false;
					if(SysSyntaskDo.TaskCodeType.BUYER_GROUP_MOBILE_UPLOAD.toString().equals(importType)){
						subResult = userDynamicGroupService.importBuyersByMobile(userDynamicGroup, buyerNickList.toArray(new String[0]));
					}else{
						subResult = userDynamicGroupService.importBuyers(userDynamicGroup, buyerNickList.toArray(new String[0]));
					}

					if(!subResult)
						return subResult;
				}
				
				try{
					in.close();
				}catch(Exception e){					
				}finally{
					in = null;
				}
				java.net.URL ossUrl = ossAccessClient.generateOssObjectUrl( "wjbcrm",file,DateUtils.nextDays(new Date(), 7) );
				if(ossUrl != null)
					urlList.add(ossUrl.toString());
			}
			if(urlList.size() > 0)
				userDynamicGroup.setMemos(urlList.get(0));
			
            return true;
		}catch(Exception e){
			logger.error("Error found when read buyer info from file",  e);
		}finally{
			if(in != null){
				try{
					in.close();
				}catch(Exception e){					
				}finally{
					in = null;
				}
			}
		}
		return false;
	}	

}
