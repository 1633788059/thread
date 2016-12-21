package com.wangjubao.app.others.service.downcenter.impl;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.quote.AlwaysQuoteMode;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wangjubao.app.others.downcenter.main.DownCenterMain;
import com.wangjubao.core.util.BuyerSearchContext;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dal.extend.bo.AreaBo;
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
import com.wangjubao.framework.util.StrUtil;

@Service("buyerGroupDownService")
public class BuyerGroupDownServiceImpl extends AbstractDownCenterServiceImpl {

	static Logger logger = LoggerFactory.getLogger("downcenter");
	
	private static final String PARENTFILE="buyerGroupDown/";
	
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
	@Autowired
	private AreaBo areaBo;
	
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
	public void init(SysSyntaskDo sysSyntaskDo) {
		sysSyntaskDo.setStatus("init");
		sellerService.updateSysSyntaskDo(sysSyntaskDo);
	}

	@Override
	public void job(SysSyntaskDo task) {
		path ="conf/ExportReport.csv";
		try {
			InetAddress addr = InetAddress.getLocalHost();
			serverIp=addr.getHostAddress().toString(); //获取本机ip 
			
		} catch (Exception e) {
			
		}
		String jsonStr = task.getParam();
		logger.info("##>>BuyerDown>>>" + jsonStr);
		JsonParser parser = new JsonParser();
		JsonObject json = parser.parse(jsonStr).getAsJsonObject();
		Long groupId = json.get("groupId").getAsLong();
		String types = json.get("types").getAsString();
		String typesName= json.get("typesName")!=null ? json.get("typesName").getAsString():"";
		try {
			task.setOwnerId(serverIp);
			task.setStatus("doing");
			
			downFile(task, groupId, types,typesName);
		} catch (Exception e) {
			task.setStatus("fail");
			task.setTitle(task.getTitle()+" < 是否分组数据无效?请重新筛选一下再试试 >");
			e.printStackTrace();
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


	/* (non-Javadoc)
	 * @see com.wangjubaovfive.service.BuyerTaskService#downFile(com.wangjubao.dolphin.biz.model.SysSyntaskDo, java.lang.Long, java.lang.String)
	 */
	public void downFile(SysSyntaskDo task, Long groupId, String types,String typesName)
			throws Exception {

		Long sellerId=task.getSellerId();
		UserDynamicGroupDo userDynamicGroup = new UserDynamicGroupDo();
		userDynamicGroup.setGroupId(groupId);
		userDynamicGroup.setSellerId(sellerId);
		userDynamicGroup = userDynamicGroupDao.load(sellerId, groupId);
		List<String> quyerFieldJsonList = new ArrayList<String>();

		String now = JodaTime.formatDate(new Date(), "MMddHHmmssSSS");
		String fileFullName=path;
//		fileFullName = fileFullName.replace(".csv", sellerId+"_"+groupId+"_"+now+".csv");
 		fileFullName = fileFullName.replace(".csv", userDynamicGroup.getGroupName().replace("/", "_")+"_"+now+".csv")
 								   .replace("ExportReport", "");

//		System.out.println("==>"+fileFullName);
		final File file = new File(fileFullName);
//		System.out.println("****>>"+file.getPath());
	
		String[] colomns = { "昵称", "姓名", "生日", "性别", "电话", "购买次数", "宝贝总数",
				"成交金额（包含运费）", "地址", "省份", "城市", "地区", "会员等级" };
		if (isNotEmpty(types)) {
			if ("mobile".equals(types)) {
				colomns[4] = "电话";
			} else if ("email".equals(types)) {
				colomns[4] = "邮件";
			} else if ("alipay".equals(types)) {
				colomns[4] = "支付宝";
			} else if ("all".equals(types)) {
				colomns[4] = "电话,邮件";
			}else {
				colomns = typesName.split(",");
			}
		}

		List<Long> userList = new ArrayList<Long>();
		if (userDynamicGroup.getSourceFrom() != null
				&& userDynamicGroup.getSourceFrom() == 1) {// 分页读取写入
			userList = buyerImportService.listAllIdsByGroup(
					userDynamicGroup.getSellerId(),
					userDynamicGroup.getGroupId());
		} else {
			JsonParser parser = new JsonParser();
			String fieldValues = userDynamicGroup.getConditions();
			String fieldValues1 = fieldValues.replace("'..", "\"..");
			fieldValues = fieldValues1.replace("gif'", "gif\"");
			JsonArray ja = parser.parse(fieldValues).getAsJsonArray();
			if (ja != null && ja.size() > 0) {
				String[] fieldValue2 = new String[ja.size()];
				for (int i = 0; i < ja.size(); i++) {
					fieldValue2[i] = ja.get(i).getAsJsonObject().toString();
				}
				if (fieldValue2 != null && fieldValue2.length > 0) {
					for (String str : fieldValue2) {
						if (null != str && str.length() > 0) {
							quyerFieldJsonList.add(str);
						}
					}
				}
			}
			
	
			
			if ("mobile".equals(types)) {
				quyerFieldJsonList
						.add("{'must':'must','field':'mobile_status','value':'1','display':'有'}");
			} else if ("email".equals(types)) {
				quyerFieldJsonList
						.add("{'must':'must','field':'email_status','value':'1','display':'有'}");
			} else if ("alipay".equals(types)) {
				quyerFieldJsonList
						.add("{'must':'must','field':'buyer_alipay_no_status','value':'1','display':'有'}");
			} else if ("all".equals(types)) {

			} else {
				if(types.indexOf("buyer_nick") < 0)
					types = types + ",buyer_nick";
			}
	
			boolean isSucess = false;
			PageQuery pageQuery = null;
			 //修改文件导出写入方式
			CsvListWriter csvWriter = null;
			PrintWriter pw = null;
			try {
				Integer countBuyer = getDolphinBuyerSearcher().searchBuyerCount(sellerId, quyerFieldJsonList);
				logger.info("Buyer count of the group :{}", countBuyer);
				if(!file.exists())
					logger.info("文件路径：------>"+fileFullName);
					file.createNewFile();
				pw = new PrintWriter(file, "GBK");
				CsvPreference.Builder preferBuilder = new CsvPreference.Builder(CsvPreference.EXCEL_PREFERENCE).useQuoteMode(new AlwaysQuoteMode());
				CsvPreference csvPreference = preferBuilder.build();
				csvWriter = new CsvListWriter(pw, csvPreference);
				csvWriter.writeHeader(colomns);//表头
				int pageNo = 1;
				int pageSize = 5000;
				int pageCount = countBuyer.intValue()/pageSize + 1;
				List<HashMap> userLists = null;
				if ("mobile".equals(types)||"email".equals(types)||"alipay".equals(types)||"all".equals(types)) {
					do {
						pageQuery = new PageQuery(pageNo, pageSize);
						userLists = getDolphinBuyerSearcher()
								.searchData(sellerId, quyerFieldJsonList,
										"buyer_id", null, fl, pageQuery);
						logger.info("Buyer count from buyer search for page {}:{}", pageNo, userLists.size());
						List<String[]> rowList = getRow(userLists, types);
						for(String[] row : rowList){
							csvWriter.write(row);	
						}
						logger.info("------a----->"+pageNo);
						userLists.clear();
						pageNo ++;
					} while (pageNo <= pageCount);
				}else {
					do {
						pageQuery = new PageQuery(pageNo, pageSize);
						userLists = getDolphinBuyerSearcher()
								.searchData(sellerId, quyerFieldJsonList,
										"buyer_id", null, types, pageQuery);
						logger.info("Buyer count from buyer search for page {}:{}", pageNo, userLists.size());
						List<String[]> rowList = getRowOther(userLists, types);
						for(String[] row : rowList){
							csvWriter.write(row);	
						}
						logger.info("------b---write-->"+pageNo);
					   userLists.clear();
					   pageNo ++;
					} while (pageNo <= pageCount);
				}
				if(csvWriter != null) {
					csvWriter.close();
				}
		        logger.info("User export file had been saved,"+fileFullName);	
		        		
				isSucess = true;
			//	String key=PARENTFILE+sellerId+"_"+groupId+"_"+System.currentTimeMillis()+".csv.zip"; 
				String key=PARENTFILE+ fileFullName +".csv.zip"; 
				File zipFile = ossAccessClient.zip(file, fileFullName);
				OSSResponse rsp = ossAccessClient.put("wjbcrm",key, zipFile,DateUtils.nextDays(new Date(), 7) );
				file.delete();
				zipFile.delete();
				task.setTitle(task.getTitle()+" [会员数约："+countBuyer+"]");
				task.setData((rsp.getUrlOss() + "").replace("wjbcrm.oss-internal.aliyuncs.com","wjbcrm.oss.aliyuncs.com"));
				task.setOwnerId(rsp.getKeyVlaue());
				task.setDescription(task.getDescription()+"  key:"+rsp.getKeyVlaue());
//				System.out.println(rsp.getUrlOss());
				task.setStatus("done");
				
			} catch (Throwable e) {
				task.setStatus("fail");
				task.setData(e.getMessage());
				isSucess = false;
				logger.error("Fail to execute buyer group down", e);
				StrUtils.showError(e);
			} finally { 
				if (null != pw) {
					pw.flush();
					pw.close();
				}
			}
			try{
				if(task.getTitle().length()>200){
					task.setTitle(task.getTitle().substring(0,200));
				}
				sellerService.updateSysSyntaskDo(task);
			}catch(Exception ex){
				task = sellerService.loadSysSyntaskDo(task);
				task.setData(ex.getMessage());
			}
		}
		
	}
	/*
	//改之前的下载文件方法zmn8-14
	public void downFile_old(SysSyntaskDo task, Long groupId, String types,String typesName)
			throws Exception {

		Long sellerId=task.getSellerId();
		if (null == areaMap) {
			AreaDo areaDo = new AreaDo();
			List<AreaDo> areaList = this.areaDao.listArea(areaDo);
			areaMap = new HashMap<String, String>();
			if (areaList != null) {
				for (AreaDo areas : areaList) {
					areaMap.put(areas.getId().toString(), areas.getName());
				}
			}
		}

		
		UserDynamicGroupDo userDynamicGroup = new UserDynamicGroupDo();
		userDynamicGroup.setGroupId(groupId);
		userDynamicGroup.setSellerId(sellerId);
		userDynamicGroup = userDynamicGroupDao.load(sellerId, groupId);
		List<String> quyerFieldJsonList = new ArrayList<String>();

		String now = JodaTime.formatDate(new Date(), "MMddHHmmssSSS");
		String fileFullName=path;
		fileFullName = fileFullName.replace(".csv", sellerId+"_"+groupId+"_"+now+".csv");

//		System.out.println("==>"+fileFullName);
		final File file = new File(fileFullName);
//		System.out.println("****>>"+file.getPath());
	
		String[] colomns = { "昵称", "姓名", "生日", "性别", "电话", "购买次数", "宝贝总数",
				"成交金额（包含运费）", "地址", "省份", "城市", "会员等级" };
		if (isNotEmpty(types)) {
			if ("mobile".equals(types)) {
				colomns[4] = "电话";
			} else if ("email".equals(types)) {
				colomns[4] = "邮件";
			} else if ("alipay".equals(types)) {
				colomns[4] = "支付宝";
			} else if ("all".equals(types)) {
				colomns[4] = "电话,邮件";
			}else {
				colomns = typesName.split(",");
			}
		}

		List<Long> userList = new ArrayList<Long>();
		if (userDynamicGroup.getSourceFrom() != null
				&& userDynamicGroup.getSourceFrom() == 1) {// 分页读取写入
			userList = buyerImportService.listAllIdsByGroup(
					userDynamicGroup.getSellerId(),
					userDynamicGroup.getGroupId());
		} else {
			JsonParser parser = new JsonParser();
			String fieldValues = userDynamicGroup.getConditions();
			String fieldValues1 = fieldValues.replace("'..", "\"..");
			fieldValues = fieldValues1.replace("gif'", "gif\"");
			JsonArray ja = parser.parse(fieldValues).getAsJsonArray();
			if (ja != null && ja.size() > 0) {
				String[] fieldValue2 = new String[ja.size()];
				for (int i = 0; i < ja.size(); i++) {
					fieldValue2[i] = ja.get(i).getAsJsonObject().toString();
				}
				if (fieldValue2 != null && fieldValue2.length > 0) {
					for (String str : fieldValue2) {
						if (null != str && str.length() > 0) {
							quyerFieldJsonList.add(str);
						}
					}
				}
			}
			
	
			
			if ("mobile".equals(types)) {
				quyerFieldJsonList
						.add("{'must':'must','field':'mobile_status','value':'1','display':'有'}");
			} else if ("email".equals(types)) {
				quyerFieldJsonList
						.add("{'must':'must','field':'email_status','value':'1','display':'有'}");
			} else if ("alipay".equals(types)) {
				quyerFieldJsonList
						.add("{'must':'must','field':'buyer_alipay_no_status','value':'1','display':'有'}");
			} else if ("all".equals(types)) {

			}
	
			boolean isSucess = false;
			PageQuery pageQuery = null;
			 
			PrintWriter write = null;
			try {
				Integer countBuyer = getDolphinBuyerSearcher().searchBuyerCount(sellerId, quyerFieldJsonList);
				if(!file.exists())
					file.createNewFile();
				write = new PrintWriter(file,"GBK");  
				
				StringBuffer csvStr = new StringBuffer();
				for (int i = 0; i < colomns.length; i++) {
					csvStr.append(colomns[i]);
					if (i < colomns.length - 1)
						csvStr.append(split);
				}
				csvStr.append("\n");
				write.write(csvStr.toString()); 
				
				int pageNo = 1;
				List<HashMap> userLists = null;
				if ("mobile".equals(types)||"email".equals(types)||"alipay".equals(types)||"all".equals(types)) {
					do {
						csvStr=new StringBuffer();
						pageQuery = new PageQuery(pageNo++, 1000);
						userLists = getDolphinBuyerSearcher()
								.searchData(sellerId, quyerFieldJsonList,
										"buyer_id", null, fl, pageQuery);
						csvStr.append(toCsv(userLists, types, areaMap));
						write.write(csvStr.toString());
		                write.flush();
//		                System.out.println(".......................");
						if (null == userLists || userLists.size() < 1000) {
							break;
						}else
							userLists=null;
					} while (true);
				}else {
					do {
						csvStr=new StringBuffer();
						pageQuery = new PageQuery(pageNo++, 1000);
						userLists = getDolphinBuyerSearcher()
								.searchData(sellerId, quyerFieldJsonList,
										"buyer_id", null, types, pageQuery);
						csvStr.append(toCsvOther(userLists, types, areaMap));
						write.write(csvStr.toString());
		                write.flush();
//		                System.out.println(".......................");
						if (null == userLists || userLists.size() < 1000) {
							break;
						}else
							userLists=null;
					} while (true);
				}
				
		        logger.info("User export file had been saved,"+fileFullName);				
		
				isSucess = true;
				String key=PARENTFILE+sellerId+"_"+groupId+"_"+System.currentTimeMillis()+".csv.zip"; 
				File zipFile = ossAccessClient.zip(file, fileFullName);
				OSSResponse rsp = ossAccessClient.put("wjbcrm",key, zipFile,DateUtils.nextDays(new Date(), 7) );
				file.delete();
				zipFile.delete();
				task.setTitle(task.getTitle()+" [会员数约："+countBuyer+"]");
				task.setData(rsp.getUrlOss()+"");
				task.setOwnerId(rsp.getKeyVlaue());
				task.setDescription(task.getDescription()+"  key:"+rsp.getKeyVlaue());
//				System.out.println(rsp.getUrlOss());
				csvStr=null;
				
				task.setStatus("done");
			} catch (Exception e) {
				task.setStatus("fail");
				task.setData(e.getMessage());
				isSucess = false;
				e.printStackTrace();
			} finally { 
				if(null!=write){
					write.flush();
					write.close();
				}
			}
			try{
				if(task.getTitle().length()>200){
					task.setTitle(task.getTitle().substring(0,200));
				}
				sellerService.updateSysSyntaskDo(task);
			}catch(Exception ex){
				task = sellerService.loadSysSyntaskDo(task);
				task.setData(ex.getMessage());
			}
		}
		
	}
	*/
	/* (non-Javadoc)
	 * @see com.wangjubaovfive.service.BuyerTaskService#isNotEmpty(java.lang.String)
	 */
	public boolean isNotEmpty(String s) {
		if (null == s || s.length() == 0)
			return false;
		return true;
	}

	public String str(Object s, String defaultValue) {
		if (null == s || "-1".equals(s.toString()))
			return defaultValue == null ? "" : defaultValue;
		return String.valueOf(s);
	}

	private String split = ",";
	String fl = "nick,real_name,birthday,gender,mobile,email,purchase_times,pay_items,payment,address,province,city,grade,buyer_alipay_no,buyer_id,buyer_nick,district";

	/* (non-Javadoc)
	 * @see com.wangjubaovfive.service.BuyerTaskService#toCsv(java.util.List, java.lang.String, java.util.Map)
	 */
	public StringBuffer toCsv(List<HashMap> list, String types,
			Map<String, String> areaMap) throws IOException {

		String[] key = fl.split(",");
		StringBuffer csv = new StringBuffer();
		if(null!=list){
		for (HashMap buyer : list) {
			Object buyerNick = buyer.get(key[0]);
			if(buyerNick == null)
				buyerNick = buyer.get(key[15]);
			csv.append(str(buyerNick, null)).append(split);
			csv.append(str(buyer.get(key[1]), null)).append(split);
			csv.append(str(buyer.get(key[2]), null)).append(split);
			int gender = Integer.valueOf(str(buyer.get(key[3]), "0"));
			csv.append(gender == 1 ? "男" : (gender == 2 ? "女" : "")).append(
					split);
			if ("email".equals(types)) {
				csv.append(str(buyer.get(key[5]), null)).append(split);
			} else if ("alipay".equals(types)) {
				csv.append(str(buyer.get(key[13]), null)).append(split);
			} else if ("mobile".equals(types)) {
				csv.append(str(buyer.get(key[4]), null)).append(split);
			} else {
				csv.append(str(buyer.get(key[4]), null)).append(split);
				csv.append(str(buyer.get(key[5]), null)).append(split);
			}

			csv.append(str(buyer.get(key[6]), "0")).append(split);// buyer.getPurchaseTimes()
			csv.append(str(buyer.get(key[7]), "0")).append(split);// buyer.getPayItems()
			csv.append(str(buyer.get(key[8]), "0")).append(split);
			csv.append(
					str(buyer.get(key[9]), "").replace(" ", "")
							.replace("\n", "").replace("\r", "")).append(split);
			String province = areaMap.get(str(buyer.get(key[10]), "无"));
			String city = areaMap.get(str(buyer.get(key[11]), "无"));
			String district = areaMap.get(str(buyer.get(key[16]), "无"));
			csv.append(province == null ? "无" : province).append(split);
			csv.append(city == null ? "无" : city).append(split);
			csv.append(district == null ? "无" : district).append(split);
			int grade = Integer.valueOf(str(buyer.get(key[12]), "0"));
			csv.append(s(grade)).append("\n"); 
		}}
		return csv;
	}
	//获取每行数据
	protected List<String[]> getRow(List<HashMap> list, String types) throws IOException {
		List<String[]> rateList = new ArrayList<String[]>();	
		String[] key = fl.split(",");
		if(null!=list){
			for (HashMap buyer : list) {
				List<String> columnList = new ArrayList<String>();
				Object buyerNick = buyer.get(key[0]);
				if(buyerNick == null)
					buyerNick = buyer.get(key[15]);
				columnList.add(str(buyerNick, null));
				columnList.add(str(buyer.get(key[1]), null));
				columnList.add(str(buyer.get(key[2]), null));
				int gender = Integer.valueOf(str(buyer.get(key[3]), "0"));
				columnList.add(gender == 1 ? "男" : (gender == 2 ? "女" : ""));
				if ("email".equals(types)) {
					columnList.add(str(buyer.get(key[5]), null));
				} else if ("alipay".equals(types)) {
					columnList.add(str(buyer.get(key[13]), null));
				} else if ("mobile".equals(types)) {
					columnList.add(str(buyer.get(key[4]), null));
				} else {
					columnList.add(str(buyer.get(key[4]), null));
					columnList.add(str(buyer.get(key[5]), null));
				}
				columnList.add(str(buyer.get(key[6]), "0"));
				columnList.add(str(buyer.get(key[7]), "0"));
				columnList.add(str(buyer.get(key[8]), "0"));
				columnList.add(
						str(buyer.get(key[9]), "").replace(" ", "")
						.replace("\n", "").replace("\r", ""));
				
				String province = str(buyer.get(key[10]), "无");
				String city = str(buyer.get(key[11]), "无");
				String district = str(buyer.get(key[16]), "无");
				columnList.add(province.equals("无")? "无" : AreaBo.getAreaNameById(Integer.valueOf(province)));
				columnList.add(city.equals("无") ? "无" : AreaBo.getAreaNameById(Integer.valueOf(city)));
				columnList.add(district.equals("无") ? "无" : AreaBo.getAreaNameById(Integer.valueOf(district)));
				int grade = Integer.valueOf(str(buyer.get(key[12]), "0"));
				columnList.add(s(grade));
				
				rateList.add(columnList.toArray(new String[0]));
			}
		}
		return rateList;
	}
	
	public StringBuffer toCsvOther(List<HashMap> list, String types,
			Map<String, String> areaMap) throws IOException {

		String[] key = types.split(",");
		StringBuffer csv = new StringBuffer();
		if(null!=list){
		for (HashMap buyer : list) {
			for (int i = 0; i < key.length; i++) {
				if ("gender".equals(key[i])) {
					
					int gender = Integer.valueOf(str(buyer.get(key[i]), "0"));
					csv.append(gender == 1 ? "男" : (gender == 2 ? "女" : ""));
					
				}else if ("grade".equals(key[i])) {
					
					int grade = Integer.valueOf(str(buyer.get(key[i]), "0"));
					csv.append(s(grade)); 
					
				}else if ("pay_items".equals(key[i])||"payment".equals(key[i])||"refund_fee".equals(key[i])||"created_payment".equals(key[i])
						||"bad_rate_num".equals(key[i]) ||"created_trades".equals(key[i])||"created_no_pay_trades".equals(key[i])||"refund_times".equals(key[i]))
				{
					
					csv.append(str(buyer.get(key[i]), "0"));
					
				}else if ("province".equals(key[i]) ) {
					
					String province = areaMap.get(str(buyer.get(key[i]), "无"));
					csv.append(province == null ? "无" : province);
					
				}else if ( "city".equals(key[i])) {
					
					String city = areaMap.get(str(buyer.get(key[i]), "无"));
					csv.append(city == null ? "无" : city);
				}else if ("first_pay_date".equals(key[i]) || "last_pay_date".equals(key[i]) || "last_login_time".equals(key[i])|| "created".equals(key[i])) {
					if (buyer.get(key[i])!=null && buyer.get(key[i]).toString().length()>11) {
						csv.append(buyer.get(key[i]).toString().substring(0,4)+"-"+buyer.get(key[i]).toString().substring(4, 6)+"-"+buyer.get(key[i]).toString().substring(6, 8)+" "+buyer.get(key[i]).toString().substring(8, 10)+":"+buyer.get(key[i]).toString().substring(10, 12));
					}
				}else {
					csv.append(str(buyer.get(key[i]), null));
				}
				if (i<key.length-1) {
					csv.append(split);
				}
			}
			csv.append("\n"); 
		}}
		return csv;
	}
	protected List<String[]> getRowOther(List<HashMap> list, String types) throws IOException {
		List<String[]> rateList = new ArrayList<String[]>();
		String[] key = types.split(",");
		if(null!=list){
			for (HashMap buyer : list) {
				List<String> columnList = new ArrayList<String>();
				for (int i = 0; i < key.length; i++) {
					if ("gender".equals(key[i])) {
						int gender = Integer.valueOf(str(buyer.get(key[i]), "0"));
						columnList.add(gender == 1 ? "男" : (gender == 2 ? "女" : ""));
					}else if ("grade".equals(key[i])) {
						int grade = Integer.valueOf(str(buyer.get(key[i]), "0"));
						columnList.add(s(grade));
					}else if ("pay_items".equals(key[i])||"payment".equals(key[i])||"refund_fee".equals(key[i])||"created_payment".equals(key[i])
							||"bad_rate_num".equals(key[i]) ||"created_trades".equals(key[i])||"created_no_pay_trades".equals(key[i])||"refund_times".equals(key[i]))
					{
						columnList.add(str(buyer.get(key[i]), "0"));
					}else if ("province".equals(key[i]) ) {
						String province = str(buyer.get(key[i]), "无");
						columnList.add(province.equals("无") ? "无" : AreaBo.getAreaNameById(Integer.valueOf(province)));
					}else if ( "city".equals(key[i])) {	
						String city = str(buyer.get(key[i]), "无");
						columnList.add(city.equals("无") ? "无" : AreaBo.getAreaNameById(Integer.valueOf(city)));
					}else if ( "district".equals(key[i])) {
						String district = str(buyer.get(key[i]), "无");
						columnList.add(district.equals("无") ? "无" : AreaBo.getAreaNameById(Integer.valueOf(district)));
					}else if ("first_pay_date".equals(key[i]) || "last_pay_date".equals(key[i]) || "last_login_time".equals(key[i])|| "created".equals(key[i])) {
						if (buyer.get(key[i])!=null && buyer.get(key[i]).toString().length()>11) {
							columnList.add(buyer.get(key[i]).toString().substring(0,4)+"-"+buyer.get(key[i]).toString().substring(4, 6)+"-"+buyer.get(key[i]).toString().substring(6, 8)+" "+buyer.get(key[i]).toString().substring(8, 10)+":"+buyer.get(key[i]).toString().substring(10, 12));
						}else {
							columnList.add("无");  // 防止错位
						}
					}else if ("nick".equals(key[i])) {
						Object buyerNick = buyer.get(key[i]);
						if(buyerNick == null)
							buyerNick = buyer.get("buyer_nick");
						columnList.add(buyerNick == null ? "" : buyerNick.toString());
					}else if ("birthday".equals(key[i])) {
						if (buyer.get(key[i])!=null &&
								(buyer.get(key[i]).toString().length()==3 ||buyer.get(key[i]).toString().length()==4)){
							columnList.add( this.convertDate(buyer.get(key[i]).toString()));
						}else {
							columnList.add("无");  // 防止错位
						}
						
					}else if ("buyer_nick".equals(key[i])) {
						//skip
					}else {
						columnList.add(str(buyer.get(key[i]), null));
					}
				}
				rateList.add(columnList.toArray(new String[0]));
			}
		}
		return rateList;
	}
	
	private  String convertDate(String dateStr){
		if (dateStr==null || dateStr.length()<3) {
			return "无";
		}
		if (dateStr.length()==3) {
			return dateStr.substring(0,1)+"月" +dateStr.substring(1,3) +"日";
		}else if(dateStr.length()==4){
			return dateStr.substring(0,2)+"月" +dateStr.substring(2,4) +"日";
		}
		return "无";
	}


	public String s(int grade) {
		if (grade == 1) {
			return "普通会员";
		} else if (grade == 2) {
			return "高级会员";
		} else if (grade == 3) {
			return "VIP会员";
		} else if (grade == 4) {
			return "至尊VIP";
		} else {
			return "";
		}
	}
	
	public static void main(String[] args) {
//		System.out.println(convertDate("826"));
//		System.out.println(convertDate("1222"));
	}
}
