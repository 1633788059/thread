package com.wangjubao.app.others.service.downcenter.impl;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wangjubao.app.others.downcenter.main.DownCenterMain;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dao.ItemSaleReportDao;
import com.wangjubao.dolphin.biz.model.BuyerRepayDo;
import com.wangjubao.dolphin.biz.model.SysSyntaskDo;
import com.wangjubao.dolphin.biz.oss.OSSAccessClient;
import com.wangjubao.dolphin.biz.oss.OSSResponse;
import com.wangjubao.dolphin.biz.service.BuyerRepayService;
import com.wangjubao.dolphin.biz.service.SellerService;
import com.wangjubao.dolphin.common.lang.Paginator;
import com.wangjubao.dolphin.common.util.JodaTime;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.StrUtils;
import com.wangjubao.dolphin.common.util.date.DateUtils;

@Service("itemRebuyDownService")
public class ItemRebuyDownServiceImpl extends AbstractDownCenterServiceImpl {
	static Logger logger = Logger.getLogger("downcenter");
	
	private static final String PARENTFILE="itemRebuyDown/";


	
	OSSAccessClient ossAccessClient = new OSSAccessClient();
	@Autowired
	private SellerService sellerService;
	
	@Autowired
	private BuyerRepayService buyerRepayService;
	
	@Autowired
    @Qualifier("itemSaleReportDao")
    private ItemSaleReportDao itemSaleReportDao;
	
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
				task.getSellerId()+"["+task.getTitle()+"]");

		String jsonStr = task.getParam();
		JsonParser parser = new JsonParser();
		JsonObject json = parser.parse(jsonStr).getAsJsonObject();
		String sourceItemId = null;
		JsonElement element = json.get("sourceItemId");
		if (element != null)
			sourceItemId = element.getAsString();
		JsonElement element1 = json.get("startTimeLong");
		Long startTimeLong = null;
		if (element1 !=null&&!"null".equals(element1.toString())){
			startTimeLong = element1.getAsLong();	
		}
		logger.info("sourceItemId:"+sourceItemId+" starttime:"+startTimeLong);
		String now = JodaTime.formatDate(new Date(), "MMddHHmmss");
		String fileName = new StringBuilder("ItemRebuy_")
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
			csvWriter.writeHeader("商品名称","购买人数","回购人数","回购比例","平均回购周期","回购周期（天）","回购人数");
//			logger.info("csv line count" + csvWriter.getLineNumber());
//			logger.info("file header count" + file.length());
//			logger.info("begin date:" + beginDate);
//			logger.info("end date:" + endDate);
			
			BuyerRepayDo buyerRepayDo = new BuyerRepayDo();
			buyerRepayDo.setSellerId(task.getSellerId());
			if (StrUtils.isNotEmpty(sourceItemId)) {
				buyerRepayDo.setSourceItemId(sourceItemId);
			}
			
			PageQuery pageQuerySearch = new PageQuery(0, 200);
			while (true) {
				PageList<BuyerRepayDo> list = this.buyerRepayService.listBuyerRepay(buyerRepayDo, pageQuerySearch,startTimeLong);
				if (list==null || list.isEmpty()) {
					break;
				}
				
				for (BuyerRepayDo buyerRepayDo2 : list) {
					String title = buyerRepayDo2.getTitle()+"";
					String payBuyer = buyerRepayDo2.getPayBuyers()+"";
					String backPayBuyers = buyerRepayDo2.getBackPayBuyers()+"";
					String backRate = "0%";
					if (buyerRepayDo2.getPayBuyers()!=null && buyerRepayDo2.getPayBuyers()!=0l && buyerRepayDo2.getBackPayBuyers()!=null) {
						Long payBuyerStringLong =buyerRepayDo2.getPayBuyers();
						backRate = new BigDecimal(buyerRepayDo2.getBackPayBuyers().toString()).divide(new BigDecimal(payBuyerStringLong.toString()), 4,
	                            BigDecimal.ROUND_HALF_DOWN).multiply(new BigDecimal("100")).toString();
					}
					//String noBackPayBuyers = buyerRepayDo2.getNoBackPayBuyers()+"";
					String avgBackPayDay = buyerRepayDo2.getAvgBackPayDay()+"";
					if (StrUtils.isNotEmpty(buyerRepayDo2.getSourceItemId())) {
						List<BuyerRepayDo> detailList = this.itemSaleReportDao.listRepayDetailByday(task.getSellerId(), buyerRepayDo2.getSourceItemId(),startTimeLong);
						if (detailList!=null && !detailList.isEmpty()) {
							for (int i = 0; i < detailList.size(); i++) {
								BuyerRepayDo repay = detailList.get(i);
								List<String> columnList = new ArrayList<String>(7);
								if (i==0) {
									columnList.add(title);
									columnList.add(payBuyer);
									columnList.add(backPayBuyers);
									columnList.add(backRate);
								//	columnList.add(noBackPayBuyers);
									columnList.add(avgBackPayDay);
									columnList.add("");
									columnList.add("");
									csvWriter.write(columnList);
									
									columnList = new ArrayList<String>(7);
									columnList.add("");
									columnList.add("");
									columnList.add("");
									columnList.add("");
									columnList.add("");
									columnList.add(repay.getGroupName()+"");
									columnList.add( repay.getPayBuyers().toString());
									csvWriter.write(columnList);
								}else {
									columnList.add("");
									columnList.add("");
									columnList.add("");
									columnList.add("");
									columnList.add("");
									columnList.add(repay.getGroupName()+"");
									columnList.add( repay.getPayBuyers().toString());
									csvWriter.write(columnList);
								}
								
								
							}
						}else {
							List<String> columnList = new ArrayList<String>(7);
							columnList.add(title);
							columnList.add(payBuyer);
							columnList.add(backPayBuyers);
							columnList.add(backRate);
							//columnList.add(noBackPayBuyers);
							columnList.add(avgBackPayDay);
							columnList.add("");
							columnList.add("");
							
							csvWriter.write(columnList);
							
						}
						
							
					}
					
				}
				
				 Paginator paginator = list.getPaginator();
	            if (paginator.getNextPage() == paginator.getPage()) {
	                break;
	            }
	            pageQuerySearch.increasePageNum();
			}
			logger.info("Start to write file : " + fileName);
//			System.out.println("Start to write file : " + fileName);
			String key = new StringBuilder(PARENTFILE).append("ItemRebuy_")
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
			OSSResponse rsp = ossAccessClient.put("wjbcrm",key, zipFile,DateUtils.nextDays(new Date(), 7));
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
//			logger.error(StrUtils.showError(e));
			logger.error(e.getMessage(), e);
//			System.out.println(StrUtils.showError(e));
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
	public void clean(SysSyntaskDo sysSyntaskDo) {
		// TODO Auto-generated method stub

	}

}
