package com.wangjubao.app.others.service.impl;

import java.io.File;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.csvreader.CsvReader;
import com.wangjubao.app.others.service.IHistoryTradeImportService;
import com.wangjubao.app.others.service.ITaobaoHistoryTradeCsvImportService;
import com.wangjubao.core.dao.IAreaDao;
import com.wangjubao.core.domain.buyer.SellerUser;
import com.wangjubao.core.domain.seller.TaobaoOrder;
import com.wangjubao.core.domain.seller.TaobaoTrade;
import com.wangjubao.core.domain.seller.TaobaoUser;
import com.wangjubao.core.domain.syn.HisImport;
import com.wangjubao.core.domain.syn.HisImportProcess;
import com.wangjubao.core.service.basic.ICommonService;
import com.wangjubao.core.service.buyer.BuyerAddupVO;
import com.wangjubao.core.service.buyer.IBuyerService;
import com.wangjubao.core.service.seller.ITaobaoService;
import com.wangjubao.framework.util.DateUtil;

public class TaobaoHistoryTradeCsvImportServiceImpl implements ITaobaoHistoryTradeCsvImportService {
	protected transient final static Logger logger = Logger.getLogger("others");
	ITaobaoService taobaoService;
	IAreaDao areaDao;
	IBuyerService buyerService;
	ICommonService commonService;
	IHistoryTradeImportService historyTradeImportService;
	
	public IHistoryTradeImportService getHistoryTradeImportService() {
		return historyTradeImportService;
	}

	public void setHistoryTradeImportService(
			IHistoryTradeImportService historyTradeImportService) {
		this.historyTradeImportService = historyTradeImportService;
	}

	public String importTrade(long sellerId, String basePath) {
		File[] files = historyTradeImportService.queryFileArray(sellerId, basePath);
		String uuid = UUID.randomUUID().toString();

		if (files.length < 1)
			return null;
		int successFileCount = 0;
		for (File file : files) {
			logger.info("开始处理文件:" + file.getAbsolutePath());
			if (this.parseFile(file, sellerId, uuid)) {
				historyTradeImportService.moveToOkFolder(file, historyTradeImportService.buildOkFilePath(sellerId, basePath));
				successFileCount++;
			} else {

			}
			logger.info("完成处理文件的导入:" + file.getAbsolutePath());
		}
		// 处理完成，发送处理完成标识
		// this.sendUpdateOverToQueue(uuid, sellerId);
		if (successFileCount > 0)
			try {
				historyTradeImportService.doOther(sellerId, uuid);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		else {

		}
		return uuid;
	}

	public String importTrade(File[] files, Long sellerId, String basePath, Long id) {
		if (files.length < 1)
			return null;
		HisImport imp = commonService.queryHisImportById(id,sellerId);
		imp.setStatus(HisImport.STATUS_2);
		imp.setUpdateTime(new Date());
		commonService.updateHisImportById(imp);

		int successFileCount = 0;
		String uuid = id.toString();
		for (File file : files) {
			logger.info("开始处理文件:" + file.getAbsolutePath());
			HisImportProcess process = new HisImportProcess();
			process.setParentId(id);
			process.setSellerId(sellerId);
			process.setProcessDesc("开始处理文件" + file.getName());
			process.setCreated(new Date());
			this.commonService.insertHisImportProcess(process);
			if (this.parseFile(file, sellerId, uuid)) {
				historyTradeImportService.moveToOkFolder(file, historyTradeImportService.buildOkFilePath(sellerId, basePath));
				successFileCount++;
			}
			logger.info("完成处理文件的导入:" + file.getAbsolutePath());
		}
		HisImportProcess process = new HisImportProcess();
		process.setParentId(id);
		if (successFileCount == 0) {
			process.setProcessDesc("成功处理了0个文件");
			process.setCreated(new Date());
			process.setSellerId(sellerId);
			this.commonService.insertHisImportProcess(process);

			imp = commonService.queryHisImportById(id,sellerId);
			imp.setStatus(HisImport.STATUS_7);
			imp.setUpdateTime(new Date());
			commonService.updateHisImportById(imp);
			return uuid;
		}
		// 处理完成，发送处理完成标识
		process.setSellerId(sellerId);
		process.setProcessDesc("处理文件完成开始计算中间表");
		process.setCreated(new Date());
		this.commonService.insertHisImportProcess(process);

		imp = commonService.queryHisImportById(id,sellerId);
		imp.setSellerId(sellerId);
		imp.setStatus(HisImport.STATUS_3);
		imp.setUpdateTime(new Date());
		commonService.updateHisImportById(imp);
		try {
			historyTradeImportService.doOther(sellerId, uuid);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return uuid;
	}

	protected boolean parseFile(File file, long sellerId, String uuid) {
		CsvReader reader = null;
		try {
			reader = new CsvReader(new FileInputStream(file), ',', Charset.forName("GBK"));
			reader.readRecord();
			String val[] = reader.getValues();
			reader.readRecord();
			String[] str = reader.getValues();
			logger.info(file.getAbsolutePath() + " 开始处理记录");
			long totalNum = 0;
			while (str != null && str.length > 0) {
				TaobaoTrade taobaoTrade = this.doParseTrade(str, file, sellerId, uuid);
				if (taobaoTrade != null) {
					this.sendUpdateToQueue(taobaoTrade);
				}
				totalNum++;
				if (totalNum % 1000 == 0) {
					logger.info(file.getAbsolutePath() + " 当前已处理记录：" + totalNum);
				}
				// str = reader.readNext();
				reader.readRecord();
				str = reader.getValues();
			}
			HisImportProcess process = new HisImportProcess();
			process.setSellerId(sellerId);
			process.setParentId(Long.parseLong(uuid));
			process.setProcessDesc("处理文件" + file.getName() + "成功");
			process.setCreated(new Date());
			this.commonService.insertHisImportProcess(process);
			return true;
		} catch (Throwable e) {
			HisImportProcess process = new HisImportProcess();
			process.setParentId(Long.parseLong(uuid));
			process.setProcessDesc("处理文件" + file.getName() + "失败" + e.toString());
			process.setCreated(new Date());
			process.setSellerId(sellerId);
			this.commonService.insertHisImportProcess(process);
			logger.info(e.getMessage(),e);
		} finally {
			if (reader != null)
				reader.close();
		}
		return false;
	}

	// private void sendUpdateOverToQueue(String uuid, long sellerId) {
	// BuyerAddupVO vo = new BuyerAddupVO();
	// vo.setUuid(uuid);
	// vo.setSellerId(sellerId);
	// buyerAddupProcessor.sendMsg(vo);
	// }

	private void sendUpdateToQueue(TaobaoTrade taobaoTrade) {
		BuyerAddupVO vo = new BuyerAddupVO(taobaoTrade.getSellerId(), taobaoTrade.getBuyerNick(), taobaoTrade.getPayDate(), taobaoTrade.getTid());
		StringBuilder sb = new StringBuilder();
		for (TaobaoOrder order : taobaoTrade.getOrders()) {
			sb.append(order.getTitle());
			sb.append(",");
		}
		vo.setGoodsTitles(sb.toString());
		if (taobaoTrade.getPayment() != null) {
			vo.setPayments(taobaoTrade.getPayment());
		} else {
			vo.setPayments(new BigDecimal("0"));
		}
		if (taobaoTrade.getNum() != null && taobaoTrade.getNum().longValue() != 0) {
			vo.setGoodsNum(taobaoTrade.getNum().intValue());
		}
		vo.setCreated(taobaoTrade.getCreated());
		vo.setReceiveName(taobaoTrade.getReceiverName());
		vo.setStatus(taobaoTrade.getStatus());

	}

	/**
	 * 
	 * @param input
	 *            解析后的csv行
	 * @param file
	 *            当前处理的csv文件
	 * @param sellerId
	 *            卖家id
	 * @param uuid
	 *            当前处理的流程号
	 * @return TaobaoTrade (如果不为空则TaobaoTrade的List<TaobaoOrder> orders一定要有值)
	 */
	protected TaobaoTrade parseTradeLine(String[] input, File file, long sellerId, String uuid) {
		long tid = Long.parseLong(input[0]);// 交易id
		if (historyTradeImportService.isExistsTrade(tid + "",sellerId)) {
			historyTradeImportService.saveLog(sellerId, file.getName(), HistoryTradeImportService.STATUS_TRADE_EXISTS, "交易在系统已经存在", tid, uuid);
			return null;
		}
		TaobaoTrade trade = new TaobaoTrade();
		TaobaoOrder order = new TaobaoOrder();
		trade.addOrder(order);
		try {
			// trade.setTid(tid);
			// order.setOid(tid);
			// order.setTid(tid);
			order.setOrderSourceId(tid + "");
			trade.setSellerId(sellerId);
			trade.setTradeSourceId(tid + "");
			trade.setShopType(TaobaoUser.SHOP_TYPE_TAOBAO);
			order.setSellerId(sellerId);
			order.setShopType(TaobaoUser.SHOP_TYPE_TAOBAO);

			String buyerNick = input[1];// 买家会员名
			String alipayNo = input[2];// 买家支付宝账号
			trade.setBuyerNick(buyerNick);
			order.setBuyerNick(buyerNick);
			trade.setAlipayNo(alipayNo);

			trade.setEmail(alipayNo);

			BigDecimal postFee = new BigDecimal("0");// 买家应付邮费
			if (StringUtils.isNotBlank(input[4])) {
				postFee = new BigDecimal(input[4]);
			}
			trade.setPostFee(postFee.toString());

			// 买家支付积分
			BigDecimal pointFee = new BigDecimal("0");
			if (StringUtils.isNotBlank(input[5])) {
				pointFee = new BigDecimal(input[5]);
			}
			trade.setPointFee(pointFee.longValue());

			// 总金额
			BigDecimal totalFee = new BigDecimal("0");
			if (StringUtils.isNotBlank(input[6])) {
				totalFee = new BigDecimal(input[6]);
			}
			trade.setTotalFee(totalFee.toString());
			order.setTotalFee(totalFee.toString());

			// 买家实际支付金额
			BigDecimal payment = new BigDecimal("0");
			if (StringUtils.isNotBlank(input[8])) {
				payment = new BigDecimal(input[8]);
			}
			trade.setPayment(payment);
			order.setPayment(payment.toString());

			// 买家实际支付积分
			BigDecimal realPointFee = new BigDecimal("0");
			if (StringUtils.isNotBlank(input[9])) {
				realPointFee = new BigDecimal(input[9]);
			}
			trade.setRealPointFee(realPointFee.longValue());

			// 订单状态
			String status = input[10];
			int statusInput = 1;
			if (StringUtils.isNotEmpty(status)) {
				if (!"交易成功".equals(status)) {
					statusInput = 7;
				}
			}
			trade.setStatus(statusInput);
			order.setStatus(statusInput);
			// 买家留言
			String buyerMessage = input[11];
			if (StringUtils.isNotBlank(buyerMessage)) {
				buyerMessage = buyerMessage.substring(0, buyerMessage.length() > 50 ? 50 : buyerMessage.length());
				trade.setBuyerMessage(buyerMessage);
			}
			// 收货人姓名
			String receiverName = input[12];
			trade.setReceiverName(receiverName);
			// 收货地址
			String receiverAddress = input[13];
			if (StringUtils.isNotBlank(receiverAddress)) {
				String split = "\u005E";
				int pos = receiverAddress.indexOf(split);
				if (pos > 0) {
					String str = receiverAddress.substring(pos);
					str = str.replace('\u005E', ',');
					String[] ssq = str.split(",,,");
					try {
						trade.setReceiverState(ssq[1]);
						trade.setReceiverCity(ssq[2]);
						trade.setReceiverDistrict(ssq[3]);
						trade.setReceiverAddress(ssq[4]);
					} catch (Exception ex) {
						trade.setReceiverAddress(receiverAddress);
					}
				} else {
					try {
						String[] ssq = receiverAddress.split(" ");
						trade.setReceiverState(ssq[0]);
						trade.setReceiverCity(ssq[1]);
						trade.setReceiverDistrict(ssq[2]);
						trade.setReceiverAddress(ssq[3]);
					} catch (Exception ex) {
						trade.setReceiverAddress(receiverAddress);
					}
				}
			}

			// 运送方式
			String shippingType = input[14];

			trade.setShippingType(shippingType);
			// 联系电话
			String receiverPhone = input[15];
			if (StringUtils.isNotBlank(receiverPhone)) {
				receiverPhone = receiverPhone.substring(1);
				trade.setReceiverPhone(receiverPhone);
			}
			// 联系手机
			String receiverMobile = input[16];
			if (StringUtils.isNotBlank(receiverMobile)) {
				receiverMobile = receiverMobile.trim();
				receiverMobile = receiverMobile.substring(1);
				receiverMobile = receiverMobile.replaceAll(",", "");
				if (receiverMobile.length() >= 11) {
					receiverMobile = receiverMobile.substring(0, 11);
				}
				trade.setReceiverMobile(receiverMobile);
			}
			// 订单创建时间
			String created = input[17];
			trade.setCreated(DateUtil.parse(created, "yyyy-MM-dd HH:mm:ss"));
			order.setCreated(DateUtil.parse(created, "yyyy-MM-dd HH:mm:ss"));
			// 订单付款时间
			String payDate = input[18];
			if (StringUtils.isNotBlank(payDate)) {
				trade.setPayTime(DateUtil.parse(payDate, "yyyy-MM-dd HH:mm:ss"));
				order.setPayTime(DateUtil.parse(payDate, "yyyy-MM-dd HH:mm:ss"));
				if (trade.getPayTime() != null) {
					order.buildOrderPayTime(trade.getPayTime());
					trade.setPayDate(order.getPayDate());
				}
				trade.setModified(trade.getPayTime());
			}
			// 宝贝标题
			String title = input[19];
			if (StringUtils.isNotBlank(title)) {
				int len = title.length() > 25 ? 25 : title.length();
				title = title.substring(0, len);
				order.setTitle(title);
			}
			// 宝贝数量
			String num = input[24];
			if (StringUtils.isNotBlank(num)) {
				trade.setNum(Long.parseLong(num));
				order.setNum(Long.parseLong(num));
			}
			this.saveInfo(trade, trade.getOrders().get(0));
		} catch (Throwable ex) {
			HisImportProcess process = new HisImportProcess();
			process.setSellerId(sellerId);
			process.setParentId(Long.parseLong(uuid));
			process.setProcessDesc("处理文件" + file.getName() + "产生异常,tid" + trade.getTradeSourceId());
			process.setStatus(1);
			process.setCreated(new Date());
			this.commonService.insertHisImportProcess(process);
			ex.printStackTrace();
			logger.info("处理文件：" + file.getAbsolutePath() + "异常，tid=" + trade.getTradeSourceId() + " " + ex);
			trade = null;
		}
		return trade;
	}

	private TaobaoTrade doParseTrade(String[] input, File file, long sellerId, String uuid) {
		TaobaoTrade trade = this.parseTradeLine(input, file, sellerId, uuid);
		if (trade == null)
			return null;

		return trade;

	}

	private void saveInfo(TaobaoTrade trade, TaobaoOrder order) {
		SellerUser su = null;
		Long selleruserid = historyTradeImportService.isExistsSellerUser(trade.getSellerId(), trade.getBuyerNick());
		if (selleruserid == null) {
			su = new SellerUser();
			su.setSellerId(trade.getSellerId());
			su.setBuyerNick(trade.getBuyerNick());
			if (StringUtils.isNotBlank(trade.getReceiverState()))
				su.setProvince(areaDao.getProvinceId(trade.getReceiverState()));
			if (StringUtils.isNotBlank(trade.getReceiverCity()))
				su.setCity(areaDao.getCityId(trade.getReceiverCity()));
			if (su.getProvince() == null && StringUtils.isNotBlank(trade.getReceiverCity())) {
				String parentNmae = areaDao.getStateName(trade.getReceiverCity());
				if (StringUtils.isNotBlank(parentNmae))
					su.setProvince(areaDao.getProvinceId(parentNmae));
			}
			su.setReceiverAddress(trade.getReceiverAddress());
			su.setEmail(trade.getAlipayNo());
			su.setReceiverMobile(trade.getReceiverMobile());
			su.setMobile(trade.getReceiverMobile());
			su.setMobileType(this.getMobileType(trade.getReceiverMobile()));
			su.setStatus(1);
			su.setShopType(trade.getShopType());
		}
		if (su != null) {
			su = buyerService.insertSellerUser(su);
			selleruserid = su.getId();
		}
		trade.setSellerUserId(selleruserid);
		order.setSellerUserId(selleruserid);
		trade.setOrderNum(1);
		taobaoService.insertTaobaoTrade(trade);
		order.setTid(trade.getTid());
		taobaoService.insertTaobaoOrder(order);
		trade.addOrder(order);
	}

	private String getMobileType(String mobile) {
		if (StringUtils.isEmpty(mobile))
			return null;
		if (mobile.length() >= 11) {
			return mobile.substring(0, 3);
		}
		return null;
	}

	public ITaobaoService getTaobaoService() {
		return taobaoService;
	}

	public void setTaobaoService(ITaobaoService taobaoService) {
		this.taobaoService = taobaoService;
	}

	public IAreaDao getAreaDao() {
		return areaDao;
	}

	public void setAreaDao(IAreaDao areaDao) {
		this.areaDao = areaDao;
	}

	public IBuyerService getBuyerService() {
		return buyerService;
	}

	public void setBuyerService(IBuyerService buyerService) {
		this.buyerService = buyerService;
	}

	public ICommonService getCommonService() {
		return commonService;
	}

	public void setCommonService(ICommonService commonService) {
		this.commonService = commonService;
	}

}
