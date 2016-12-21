package com.wangjubao.app.others.service.downcenter.impl;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wangjubao.app.others.downcenter.main.DownCenterMain;
import com.wangjubao.dolphin.biz.common.constant.CouponCodeStatus;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.model.CouponCodeDetailDo;
import com.wangjubao.dolphin.biz.model.CouponCodeDo;
import com.wangjubao.dolphin.biz.model.CouponCodeGiftDo;
import com.wangjubao.dolphin.biz.model.SysSyntaskDo;
import com.wangjubao.dolphin.biz.oss.OSSAccessClient;
import com.wangjubao.dolphin.biz.oss.OSSResponse;
import com.wangjubao.dolphin.biz.service.CouponCodeGiftService;
import com.wangjubao.dolphin.biz.service.CouponCodeService;
import com.wangjubao.dolphin.biz.service.SellerService;
import com.wangjubao.dolphin.common.lang.Paginator;
import com.wangjubao.dolphin.common.util.JodaTime;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.StrUtils;
import com.wangjubao.dolphin.common.util.date.DateUtils;

@Service("couponCodeDownService")
public class CouponCodeDownServiceImplo extends AbstractDownCenterServiceImpl {

	static Logger logger = Logger.getLogger("downcenter");
		
	private static final String PARENTFILE="couponCodeDown/";


	OSSAccessClient ossAccessClient = new OSSAccessClient();
	public static final SimpleDateFormat DATE_TIME = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	
	@Autowired
	private SellerService sellerService;
	@Autowired
	 private CouponCodeService couponCodeService;
	@Autowired
	private CouponCodeGiftService couponCodeGiftService;
	
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
		logger.info("Start to download item rebuy {}, {}"+
				task.getSellerId()+"["+ task.getTitle()+"]");

		String jsonStr = task.getParam();
		JsonParser parser = new JsonParser();
		JsonObject json = parser.parse(jsonStr).getAsJsonObject();
		String id = null;
		JsonElement element = json.get("id");
		if (element != null)
			id = element.getAsString();


		String now = JodaTime.formatDate(new Date(), "MMddHHmmss");
		String fileName = new StringBuilder("CouponCode_")
				.append(task.getSellerId()).append("_").append(now)
				.append(".csv").toString();

		String path = "conf/ExportReport.csv";
		
		path = path.replaceAll("ExportReport.csv", fileName);

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
			csvWriter = new CsvListWriter(pw,
					CsvPreference.STANDARD_PREFERENCE);
			logger.info("Start to write header.");
			csvWriter.writeHeader("优惠码编号","优惠码有效开始时间","优惠码有效结束时间","优惠码批次","可兑换的赠品名称","兑换状态","兑换买家");
//			logger.info("csv line count" + csvWriter.getLineNumber());
//			logger.info("file header count" + file.length());
//			logger.info("begin date:" + beginDate);
//			logger.info("end date:" + endDate);
			
			CouponCodeDo couponCodeDo = new CouponCodeDo();
			couponCodeDo.setSellerId(task.getSellerId());
			if (StrUtils.isNotEmpty(id)) {
				couponCodeDo.setId(Long.valueOf(id));
			
				couponCodeDo = this.couponCodeService.getCouponCodeBatch(task.getSellerId(), couponCodeDo);
				
				if (couponCodeDo!=null) {
					String beginString = DateUtils.formatDate(couponCodeDo.getStartTime(), "yyyy-MM-dd");
					String endString = DateUtils.formatDate(couponCodeDo.getEndTime(), "yyyy-MM-dd");
					StringBuffer giftName = new StringBuffer("");
					//获取赠品
					if (StrUtils.isNotEmpty(couponCodeDo.getGiftIds())) {
						String giftIds = couponCodeDo.getGiftIds();
						List<String> ids = null;
						if (StrUtils.isNotEmpty(giftIds)) {
							ids =new ArrayList<String>();
							String[] idsStrings = giftIds.split(",");
							for (String gift : idsStrings) {
								if (StrUtils.isNotEmpty(gift.trim())) {
									ids.add(gift);
								}
							}
						}
						 PageQuery pq = new PageQuery(1,9999);
						 CouponCodeGiftDo couponCodeGiftDo = new CouponCodeGiftDo();
				        couponCodeGiftDo.setSellerId(task.getSellerId());
						PageList<CouponCodeGiftDo> list=this.couponCodeGiftService.listGiftDosByIds(couponCodeGiftDo,ids, pq, 0);
						for (CouponCodeGiftDo couponCodeGiftDo2 : list) {
							giftName.append(couponCodeGiftDo2.getItemName()).append(",");
						}
					}
					PageQuery pageQuerySearch = new PageQuery(0, 200);
					while (true) {
						PageList<CouponCodeDetailDo> list = this.couponCodeService.listCouponCodeDetail(task.getSellerId(), couponCodeDo,pageQuerySearch);
						if (list==null || list.isEmpty()) {
							break;
						}
						
						for (CouponCodeDetailDo couponCodeDetail : list) {
							String code = couponCodeDetail.getCode()+"";
							String status = couponCodeDetail.getStatus()+"";
							String statusMemo = CouponCodeStatus.getStatus(couponCodeDetail.getStatus(),2);
							String batchIdString = couponCodeDetail.getBatchId()+"";
							String buyerNick = StrUtils.isNotEmpty(couponCodeDetail.getBuyerNick())?couponCodeDetail.getBuyerNick():"无";
							
							List<String> columnList = new ArrayList<String>(5);
							columnList.add(code);
							columnList.add(beginString);
							columnList.add(endString);
							columnList.add(batchIdString);
							columnList.add(giftName.toString());
							columnList.add(statusMemo);
							columnList.add(buyerNick);
							
							csvWriter.write(columnList);
							
						}
						
						 Paginator paginator = list.getPaginator();
			            if (paginator.getNextPage() == paginator.getPage()) {
			                break;
			            }
			            pageQuerySearch.increasePageNum();
					}
				}
			}
//			logger.info("Start to write file : " + fileName);
			System.out.println("Start to write file : " + fileName);
			String key = new StringBuilder(PARENTFILE).append("CouponCode_")
					.append(task.getSellerId()).append("_")
					.append(System.currentTimeMillis()).append(".csv.zip")
					.toString();
			if(csvWriter != null) {
				csvWriter.close();
			}
			
			File zipFile = ossAccessClient.zip(file, fileName);
			logger.info("file length: " + file.length());
			logger.info("Start to write file : " + zipFile);
			logger.info("Start to write key : " + key);
			OSSResponse rsp = ossAccessClient.put( "wjbcrm",key, zipFile,DateUtils.nextDays(new Date(), 7));
			file.delete();
			zipFile.delete();
			task.setTitle(task.getTitle());
			task.setData((rsp.getUrlOss() + "").replace("wjbcrm.oss-internal.aliyuncs.com","wjbcrm.oss.aliyuncs.com"));
			task.setOwnerId(rsp.getKeyVlaue());
			task.setDescription(task.getDescription() + "  key:"
					+ rsp.getKeyVlaue());

			task.setStatus("done");
		} catch (Exception e) {
			task.setStatus("fail");
			task.setData(e.getMessage());
			logger.error(StrUtils.showError(e));
			logger.error(e.getMessage(), e);
			System.out.println(StrUtils.showError(e));
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

}
