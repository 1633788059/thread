package com.wangjubao.app.others.service.downcenter.impl;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.quote.AlwaysQuoteMode;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dao.BuyerDao;
import com.wangjubao.dolphin.biz.model.BuyerDo;
import com.wangjubao.dolphin.biz.model.FieldDateDataDo;
import com.wangjubao.dolphin.biz.model.FieldIntDataDo;
import com.wangjubao.dolphin.biz.model.FieldTextDataDo;
import com.wangjubao.dolphin.biz.model.NaireBackfillDo;
import com.wangjubao.dolphin.biz.model.NaireElementDo;
import com.wangjubao.dolphin.biz.model.SysSyntaskDo;
import com.wangjubao.dolphin.biz.model.UserFormDo;
import com.wangjubao.dolphin.biz.oss.OSSAccessClient;
import com.wangjubao.dolphin.biz.oss.OSSResponse;
import com.wangjubao.dolphin.biz.service.NaireElementService;
import com.wangjubao.dolphin.biz.service.QuestService;
import com.wangjubao.dolphin.biz.service.QuestionnaireService;
import com.wangjubao.dolphin.biz.service.SellerService;
import com.wangjubao.dolphin.biz.service.UserFormService;
import com.wangjubao.dolphin.common.lang.Paginator;
import com.wangjubao.dolphin.common.util.JodaTime;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.StrUtils;
import com.wangjubao.dolphin.common.util.date.DateUtils;

@Service("questExportService")
public class QuestExportServiceImp extends AbstractDownCenterServiceImpl {

	static Logger logger = LoggerFactory.getLogger("downcenter");

	private static final String PARENTFILE = "questDown/";

	private  String path = "";

	private static String serverIp = "0.0.0.0";

	OSSAccessClient ossAccessClient = new OSSAccessClient();
	@Autowired
	private SellerService sellerService;

	@Autowired
	private QuestService questService;
	
	@Autowired
	private QuestionnaireService questionnaireService;
	
	@Autowired
	private NaireElementService naireElementService;

	@Autowired
	@Qualifier("dolphinBuyerDao")
	private BuyerDao buyerDao;

	@Autowired
	private UserFormService userFormService;

	@Override
	public void init(SysSyntaskDo sysSyntaskDo) {
		sysSyntaskDo.setStatus("init");
		sellerService.updateSysSyntaskDo(sysSyntaskDo);
	}

	@Override
	public void job(SysSyntaskDo task) {
		path = "conf/ExportReport.csv";
		try {
			InetAddress addr = InetAddress.getLocalHost();
			serverIp = addr.getHostAddress().toString(); // 获取本机ip

		} catch (Exception e) {

		}
		GsonBuilder builder = new GsonBuilder();
		Gson gson = builder.create();
		if (task.getDeletedFlag().intValue() == 1) {
			logger.info("Task has been removed");
			return;
		}
		task.setStatus("doing");
		//task.setOwnerId(serverIp);
		sellerService.updateSysSyntaskDo(task);
		logger.info("Start to download question  " + task.getSellerId() + "【" + task.getTitle() + "】");
		String jsonStr = task.getParam();
		JsonParser parser = new JsonParser();
		JsonObject json = parser.parse(jsonStr).getAsJsonObject();
		JsonElement element = json.get("qid");
		String qid = null;
		String range = null;
		if (element != null) {
			qid = element.getAsString();
		}
		element = json.get("range");
		if (element != null) {
			range = element.getAsString();
		}
		String now = JodaTime.formatDate(new Date(), "yyyyMMddHHmmss");
		String fileName = new StringBuilder("Quest_").append(task.getSellerId()).append('_').append(task.getId())
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
		createCSVData(Long.valueOf(qid), task.getSellerId(), range, file, task);
	}

	@Override
	public void clean(SysSyntaskDo sysSyntaskDo) {

	}

	private void createCSVData(Long qid, Long sellerId, String range, File fileName, SysSyntaskDo task) {
		List<NaireElementDo> elementList = questService.queryByQId(sellerId, qid);
		if (elementList == null || elementList.size() == 0) {
			logger.error("elementList is null....");
			return;
		}
		CsvListWriter csvWriter = null;
		PrintWriter pw = null;
		List<Map<String, String>> finalList = new ArrayList<Map<String, String>>();
		
		// Map<String,List<String>> finalList = new
		// HashMap<String,List<String>>();//最终的map
		try {
			pw = new PrintWriter(fileName, "GBK");
			CsvPreference.Builder preferBuilder = new CsvPreference.Builder(CsvPreference.EXCEL_PREFERENCE)
					.useQuoteMode(new AlwaysQuoteMode());
			CsvPreference csvPreference = preferBuilder.build();
			csvWriter = new CsvListWriter(pw, csvPreference);
			if(!"2".equals(task.getOwnerId()))
			{
						// 写头部信息
						String header = "";
						List<String> bExist=new ArrayList<String>();//判断固定选项是否存在
						for (NaireElementDo ele : elementList) {
							header += ele.getEName() + ",";
							bExist.add(ele.getEName());
						}
						csvWriter.writeHeader(header.split(","));
						// 填问卷的会员
						PageList<NaireBackfillDo> questionBuyerList = questService.buyerList(sellerId, qid);
			
						logger.info("----->填问卷的会员questionBuyerList" + JSON.toJSONString(questionBuyerList));
						List<BuyerDo> buyerList = new ArrayList<BuyerDo>();
						logger.info(range);
						// 判断会员类型:本店会员 非本店会员 全部会员
						for (NaireBackfillDo naireBackfill : questionBuyerList) {
							BuyerDo buyer = buyerDao.load(sellerId, naireBackfill.getBuyerId());
							// 本店会员：丢掉非本店会员
							if ("本店会员".equals(range)) {
								if (buyer != null) {
									buyerList.add(buyer);
								}
								// 非本店会员：丢掉本店会员
							} else if ("非本店会员".equals(range)) {
								if (buyer == null) {
									BuyerDo b = new BuyerDo();
									b.setId(naireBackfill.getBuyerId());
									buyerList.add(b);
								}
							} else {
								BuyerDo b = new BuyerDo();
								b.setId(naireBackfill.getBuyerId());
								buyerList.add(b);
							}
						} // for end
						// 遍历会员
						for (BuyerDo buyerDo : buyerList) {
							// Map<String,String> item = new HashMap<String, String>();
							// //key为title
							if (buyerDo == null || buyerDo.getId() == null) {
								logger.debug("buyer 为空");
								continue;
							}
							// 选项名字
							Map<String, String> nameMap = new HashMap<String, String>();
							nameMap.put("buyerId", buyerDo.getId().toString());
							// 反填结果
							// 固定的
							List<NaireBackfillDo> backList = questService.queryListNaire(sellerId, qid, buyerDo.getId());
							if (backList.size() > 0) {
								for (NaireBackfillDo naire : backList) {
									if (naire.getNeKey().equals("sex")) {
										nameMap.put(naire.getNeKey(), naire.getBContent().equals("1") ? "男" : "女");
									} else {
										nameMap.put(naire.getNeKey(), naire.getBContent());
									}
								}
							}
							// 所有的标签
							List<NaireElementDo> allElementList = questService.queryByQId(sellerId, Long.valueOf(qid));
							// 去掉固定的
							for (Iterator<NaireElementDo> iterator = allElementList.iterator(); iterator.hasNext();) {
								NaireElementDo element = iterator.next();
								if (!"标签".equals(element.getEMemo())) {
									iterator.remove();
								}
							}
							// 单选多选 对应关系
							// Map<String,List<String>> rcMap = new HashMap<String,
							// List<String>>();
							// 文本对应关系
							// Map<String,String> textMap = new HashMap<String,String>();
			
							for (NaireElementDo naire : allElementList) {
								UserFormDo userForm = userFormService.load(Long.valueOf(naire.getEkey()), sellerId);
								if (userForm.getStyle().equals("text")) {
									List<FieldTextDataDo> textlist = questService.queryFieldData(sellerId, buyerDo.getId(),
											userForm.getFormId());
									for (FieldTextDataDo textInx : textlist) {
										// textMap.put(userForm.getName(),
										// textInx.getDataValue());
										nameMap.put(userForm.getName(), textInx.getDataValue());
									}
								}else if (userForm.getStyle().equals("date")){
									List<FieldDateDataDo> datelist=questService.queryFieldDateData(sellerId, buyerDo.getId(),
											userForm.getFormId());
									for (FieldDateDataDo dateInx : datelist) {
										// textMap.put(userForm.getName(),
										// textInx.getDataValue());
										nameMap.put(userForm.getName(), DateUtils.formatDate(dateInx.getDataValue(),"yyyy-MM-dd"));
									}
								} else {
									List<FieldIntDataDo> fieldInt = questService.loadIntValue(sellerId, buyerDo.getId(),
											userForm.getFormId());
									Method[] methods = userForm.getClass().getMethods();
									// List<String> element_list = new ArrayList<String>();
									String elementStr = "";
									for (FieldIntDataDo field : fieldInt) {
										for (Method method : methods) {
											if (method.getName().contains("getItem" + field.getDataValue())) {
												elementStr += "," + (String) method.invoke(userForm);
												// element_list.add((String)
												// method.invoke(userForm));
												break;
											}
										}
									}
									// rcMap.put(userForm.getName(), element_list);
									if (StrUtils.isNotEmpty(elementStr)) {
										elementStr = elementStr.replaceFirst(",", "");
									}
									nameMap.put(userForm.getName(), elementStr);
								}
							}
							finalList.add(nameMap);
						} // for buerList end
			
						// 写csv 内容
						List<NaireElementDo> allElementList = questService.queryByQId(sellerId, Long.valueOf(qid));
						for (Map<String, String> map : finalList) {
							// 固定：buyernick birthday sex email mobile
							List<String> toWrite = new ArrayList<String>();
							if(bExist.contains("会员昵称"))
							{
								toWrite.add(map.get("buyernick"));
							}
							if(bExist.contains("生日"))
							{
								toWrite.add(map.get("birthday"));
							}
							if(bExist.contains("手机号码"))
							{
								toWrite.add(map.get("mobile"));
							}
							if(bExist.contains("邮箱"))
							{
								toWrite.add(map.get("email"));
							}
							if(bExist.contains("性别"))
							{
								toWrite.add(map.get("sex"));
							}
							for (NaireElementDo naire : allElementList) {
								// 去掉固定的
			//					if (naire.getEMemo() == null) {
			//						if (naire.getEkey().equals("buyernick")) {
			//							continue;
			//						}
			//					} else {
			//						if (naire.getEMemo().equals("固定")) {
			//							continue;
			//						}
			//					}
								if(!"标签".equals(naire.getEMemo())){
									continue;
								}
								UserFormDo userForm = userFormService.load(Long.valueOf(naire.getEkey()), sellerId);
								toWrite.add(map.get(userForm.getName()));
							}
							logger.debug("buyerId:" + map.get("buyerId") + "准备写入...");
							csvWriter.write(toWrite);
						}
				}
			else
			{//积分问卷的
				// 写头部信息
				String header = "会员昵称,";
				elementList=naireElementService.queryJifenNaireByQId(sellerId, Long.valueOf(qid),null);
				for (NaireElementDo ele : elementList) {
					header += ele.getEName() + ",";
				}
				csvWriter.writeHeader(header.split(","));
				
				PageQuery pageQuery = new PageQuery(1, 200);
				
				while(true)
				{
					Integer isMember=0;
					if ("本店会员".equals(range)) {
						isMember=1;
					} else if ("非本店会员".equals(range)) {
						isMember=2;
					} else {
						isMember=0;
					}
					
					PageList<NaireBackfillDo> pageList=questionnaireService.listJifenNaireFillByPage(sellerId,qid,null, isMember, pageQuery);
					for(NaireBackfillDo naireBackfillDo:pageList)
					{
						Map<String, String> nameMap = new HashMap<String, String>();
						nameMap.put("会员昵称", naireBackfillDo.getBMemo());
						List<NaireBackfillDo> problemList=questionnaireService.listByAccountId(sellerId, qid, naireBackfillDo.getAccountId());
						for(NaireElementDo naireElementDo:elementList)
						{
							String eAnswer=naireElementDo.geteAnswer();
							JSONArray  items=JSONArray.parseArray(eAnswer);
							if(problemList!=null)
							{
								for(NaireBackfillDo nairefillDo:problemList)
								{
									if(nairefillDo.getNeId().longValue()==naireElementDo.getId().longValue())
									{
										if(naireElementDo.getEType()!=1&&naireElementDo.getEType()!=4)
										{
											for(int i=0;i<items.size();i++)
											{
												Object object=items.get(i);
												JSONObject jObj=JSON.parseObject(object.toString());
												if(jObj.getString("index").equals(nairefillDo.getBContent()))
												{
													if(nameMap.containsKey(naireElementDo.getId().longValue()+""))//多选题才会有多个fill
													{
														nameMap.put(naireElementDo.getId().longValue()+"", 
																nameMap.get(naireElementDo.getId().longValue()+"")+","+jObj.getString("answer"));
													}
													else
													{
														nameMap.put(naireElementDo.getId().longValue()+"", jObj.getString("answer"));
													}
												}	
											}
										}
										else
										{
											nameMap.put(naireElementDo.getId().longValue()+"",nairefillDo.getBContent() );
										}
									}
								}
							}
						}
						finalList.add(nameMap);
					}
					for (Map<String, String> map : finalList) {
						List<String> toWrite = new ArrayList<String>();
						toWrite.add(map.get("会员昵称"));
						for (NaireElementDo naire : elementList) {
							if(map.containsKey(naire.getId()+""))
							{
								toWrite.add(map.get(naire.getId()+""));
							}
							else
							{
								toWrite.add("");
							}
						}
						logger.debug("会员昵称:" + map.get("会员昵称") + "准备写入...");
						csvWriter.write(toWrite);
					}
					
					Paginator paginator = pageList.getPaginator();
					if (paginator.getNextPage() == paginator.getPage())
		            {
		                break;
		            }
					pageQuery.increasePageNum();
				}
			}
			csvWriter.close();
			final File file = new File(path);
			String key = PARENTFILE + sellerId + "_" + qid + "_" + System.currentTimeMillis() + ".csv.zip";
			File zipFile = ossAccessClient.zip(file, path);
			OSSResponse rsp = ossAccessClient.put("wjbcrm", key, zipFile, DateUtils.nextDays(new Date(), 7));
			// file.delete();
			// zipFile.delete();
			task.setTitle(task.getTitle());
			task.setData((rsp.getUrlOss() + "").replace("wjbcrm.oss-internal.aliyuncs.com","wjbcrm.oss.aliyuncs.com"));
			//task.setOwnerId(rsp.getKeyVlaue());
			task.setDescription(task.getDescription() + "  key:" + rsp.getKeyVlaue());
			System.out.println(rsp.getUrlOss());
			task.setStatus("done");
			sellerService.updateSysSyntaskDo(task);
		} catch (Exception e) {
			task.setStatus("fail");
			sellerService.updateSysSyntaskDo(task);
			task.setData(e.getMessage());
			logger.error(e.getMessage(), e);
		} finally {
			if (null != pw) {
				pw.flush();
				pw.close();
			}
		}
	}

}
