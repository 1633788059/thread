package com.wangjubao.app.others.service.downcenter.impl;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.wangjubao.app.others.service.downcenter.DownCenterService;
import com.wangjubao.app.others.service.downcenter.TaskCodeTypeService;
import com.wangjubao.dolphin.biz.model.SysSyntaskDo;
import com.wangjubao.dolphin.common.util.StrUtils;

@Service
public class TaskCodeTypeServiceImpl implements TaskCodeTypeService {

	static Logger logger = Logger.getLogger("downcenter");
	@Autowired
	@Qualifier("buyerGroupDownService")
	private DownCenterService buyerGroupDownService;
	
	@Autowired
	@Qualifier("buyerGroupUploadService")
	private DownCenterService buyerGroupUploadService;
	
	@Autowired
	@Qualifier("buyerRateDownService")
	private DownCenterService  buyerRateDownService;
	
	@Autowired
	@Qualifier("adsBuyerRateDownService")
	private DownCenterService  adsBuyerRateDownService;
	
	@Autowired
	@Qualifier("couponCodeDownService")
	private DownCenterService couponCodeDownService;
	
	@Autowired
	@Qualifier("itemRebuyDownService")
	private DownCenterService itemRebuyDownService;
	
	@Autowired
	@Qualifier("marketingUploadService")
	private DownCenterService marketingUploadService;
	
	@Autowired
	@Qualifier("jifenInfoDownService")
	private DownCenterService jifenInfoDownService;
	
	@Autowired
	@Qualifier("questExportService")
	private QuestExportServiceImp questExportService;

	@Autowired
	@Qualifier("marketingReportDownService")
	private MarketingReportDownServiceImpl marketingReportDownService;
	
	@Override
	public DownCenterService getServiceByCodeType(String codeType) {
		if (StrUtils.isEmpty(codeType)) {
			logger.info("=================不存在的下载类别");
			return null;
		}
		SysSyntaskDo.TaskCodeType[] codes = SysSyntaskDo.TaskCodeType.values();
//		logger.info("codes is :"+codes.toString()+",,+codeType is :"+codeType);
		
		boolean isValid =false;
		for (SysSyntaskDo.TaskCodeType taskCodeType : codes) {
			if (taskCodeType.toString().equals(codeType)) {
				isValid=true;
				break;
			}
		}
		if (isValid) {
			if (SysSyntaskDo.TaskCodeType.BUYER_GROUP_DOWN.toString().equals(codeType)) {
				return buyerGroupDownService;
			}else if (SysSyntaskDo.TaskCodeType.BUYER_GROUP_UPLOAD.toString().equals(codeType) || SysSyntaskDo.TaskCodeType.BUYER_GROUP_MOBILE_UPLOAD.toString().equals(codeType)) {
				return buyerGroupUploadService;
			}else if (SysSyntaskDo.TaskCodeType.BUYER_RATE_DOWNLOAD.toString().equals(codeType)) {
				return buyerRateDownService;
			}else if (SysSyntaskDo.TaskCodeType.ADS_BUYER_RATE_DOWNLOAD.toString().equals(codeType)) {
				return adsBuyerRateDownService;
			}else if (SysSyntaskDo.TaskCodeType.COUPON_CODE_DOWNLOAD.toString().equals(codeType)) {
				return couponCodeDownService;
			}else if (SysSyntaskDo.TaskCodeType.ITEM_REPUY_DOWNLOAD.toString().equals(codeType)) {
				return itemRebuyDownService;
			}else if (SysSyntaskDo.TaskCodeType.QUEST_RECORD_EXPORT.toString().equals(codeType)) {
				return questExportService;
			}else if (SysSyntaskDo.TaskCodeType.MARKET_EMAIL_UPLOAD.toString().equals(codeType) || SysSyntaskDo.TaskCodeType.MARKET_MOBILE_UPLOAD.toString().equals(codeType)) {
				return marketingUploadService;
			}else if(SysSyntaskDo.TaskCodeType.JIFEN_INFO_DOWNLOAD.toString().equals(codeType)){
				return jifenInfoDownService;
			}else if(SysSyntaskDo.TaskCodeType.MARKETING_REPORT_DOWNLOAD.toString().equals(codeType)){
				return marketingReportDownService;
			}
		}
		return null;
	}
}
