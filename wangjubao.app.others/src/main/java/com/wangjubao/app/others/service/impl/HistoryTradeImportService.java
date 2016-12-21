package com.wangjubao.app.others.service.impl;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.util.CollectionUtils;

import com.wangjubao.app.others.service.IHistoryTradeImportService;
import com.wangjubao.core.domain.buyer.SellerUser;
import com.wangjubao.core.service.basic.ICommonService;
import com.wangjubao.core.service.buyer.BuyerAddupVO;
import com.wangjubao.core.service.datacalculate.IPreCalculateEngineService;
import com.wangjubao.core.service.datacalculate.IRebuildAllTableService;
import com.wangjubao.core.service.syn.IFacadeService;
import com.wangjubao.core.service.syn.TaobaoSynSericeService;
import com.wangjubao.core.service.syn.impl.IGroupRuleService;
import com.wangjubao.framework.util.DateUtil;

public class HistoryTradeImportService implements IHistoryTradeImportService {
	protected transient final static Logger logger = Logger.getLogger("others");
	
	public static String ORDER = "order";
	public static String ORDEROK = "orderok";
	public static int STATUS_PROCESSING = 2;
	// 最后一条交易数据被mq处理完成
	public static int STATUS_SUCCESS = 6;
	public static int STATUS_FAIL = 3;
	public static int STATUS_ROW_FAIL = 4;
	public static int STATUS_TRADE_EXISTS = 5;

	// 开始重算中间表
	public static int STATUS_MIDDLE_BUILD_BEGIN = 6;
	// 中间表重算完成
	public static int STATUS_MIDDLE_BUILD_FINISH = 7;
	// 开始重建lucene索引
	public static int STATUS_INDEX_BUILD_BEGIN = 8;
	// lucene重建完成
	public static int STATUS_INDEX_BUILD_FINISH = 9;

	IFacadeService facadeService;

	protected ICommonService commonService;

	TaobaoSynSericeService taobaoSynService;

	IGroupRuleService groupRuleService;

	IPreCalculateEngineService preCalculateEngineService;

	public IPreCalculateEngineService getPreCalculateEngineService() {
		return preCalculateEngineService;
	}

	public void setPreCalculateEngineService(
			IPreCalculateEngineService preCalculateEngineService) {
		this.preCalculateEngineService = preCalculateEngineService;
	}

	public IFacadeService getFacadeService() {
		return facadeService;
	}

	public void setFacadeService(IFacadeService facadeService) {
		this.facadeService = facadeService;
	}

	public ICommonService getCommonService() {
		return commonService;
	}

	public void setCommonService(ICommonService commonService) {
		this.commonService = commonService;
	}

	public TaobaoSynSericeService getTaobaoSynService() {
		return taobaoSynService;
	}

	public void setTaobaoSynService(TaobaoSynSericeService taobaoSynService) {
		this.taobaoSynService = taobaoSynService;
	}

	public IGroupRuleService getGroupRuleService() {
		return groupRuleService;
	}

	public void setGroupRuleService(IGroupRuleService groupRuleService) {
		this.groupRuleService = groupRuleService;
	}


	/**
	 * 返回待处理的文件列表
	 * 
	 * @param sellerId
	 * @param basePath
	 * @return
	 */
	public File[] queryFileArray(Long sellerId, String basePath) {
		String path = this.buildFilePath(sellerId, basePath);
		File folder = new File(path);
		if (!folder.isDirectory() || !folder.exists()) {
			logger.info(path + "目录不存在");
			throw new RuntimeException(path + "目录不存在");
			// return new File[]{};
		}
		return folder.listFiles();
	}

	public void updateSuccess(String uuid, int oldstatus, int status,Long sellerId) {
		String sql = " t_history_trade_log set status=" + status
				+ " where uuidstr='" + uuid + "'and status=" + oldstatus+" and sellerId"+sellerId;
		this.commonService.updateDataBySql(sql);
	}

	public void saveLog(long sellerId, String fileName, int status,
			String remark, String uuid) {
		String sql = " insert into t_history_trade_log(sellerId,fileName,status,remark,createdDate,tid,uuidstr)values("
				+ sellerId
				+ ",'"
				+ fileName
				+ "',"
				+ status
				+ ""
				+ ",'"
				+ remark
				+ "','"
				+ DateUtil.format(new Date(), "yyyy-MM-dd HH:mm:ss")
				+ "',0,'"
				+ uuid + "')";
		this.commonService.insertDataBySql(sql);
	}

	public void saveLog(long sellerId, String fileName, int status,
			String remark, long tid, String uuid) {
		String sql = " insert into t_history_trade_log(sellerId,fileName,status,remark,createdDate,tid,uuidstr)values("
				+ sellerId
				+ ",'"
				+ fileName
				+ "',"
				+ status
				+ ""
				+ ",'"
				+ remark
				+ "','"
				+ DateUtil.format(new Date(), "yyyy-MM-dd HH:mm:ss")
				+ "',"
				+ tid + ",'" + uuid + "')";
		this.commonService.insertDataBySql(sql);
	}

	public boolean isExistsTrade(String tradeSourceId,Long sellerId) {
		String sql = " select count(*) from t_taobao_trade where shopType=1 and tradeSourceId='" + tradeSourceId+"' and sellerId="+sellerId;
		Object obj = this.commonService.queryObjectBySql(sql);
		if (obj == null || Long.parseLong(obj.toString()) < 1)
			return false;
		return true;
	}

	public Long isExistsSellerUser(long sellerId, String buyernick) {
		String sql = " select id from t_seller_user where sellerid=" + sellerId
				+ " and buyernick='" + buyernick + "'";
		Object obj = this.commonService.queryObjectBySql(sql);
		if (obj == null || Long.parseLong(obj.toString()) < 1)
			return null;
		return Long.parseLong(obj.toString());
	}

	public void onAddup(BuyerAddupVO vo) {
		SellerUser su = taobaoSynService.updateSellUserSyn(vo.getSellerId(), vo
				.getBuyerNic(), vo.getModifyDate());
		try {
			// 运行规则文件，更新买家所属的分组及标签属性
			if (su != null)
				groupRuleService.doRunRule(su, vo);
		} catch (Exception e) {
			e.printStackTrace();
			logger.info("计算买家规则报错" + e.toString() + " tid=" + vo.getTid());
		}
	}

	/**
	 * 根路径
	 * 
	 * @param sellerId
	 * @param basePath
	 * @return
	 */
	public String buildFilePath(long sellerId, String basePath) {
		return basePath + File.separator + sellerId + File.separator + ORDER;

	}

	public String buildOkFilePath(long sellerId, String basePath) {
		return basePath + File.separator + sellerId + File.separator + ORDEROK;

	}

	public void moveToOkFolder(File inputFile, String destFolder) {
		File folder = new File(destFolder);
		if (!folder.exists()) {
			folder.mkdir();
		}
		File outputFile = new File(destFolder + File.separator
				+ inputFile.getName());
		if (outputFile.exists()) {
			outputFile.renameTo(new File(destFolder + File.separator
					+ DateUtil.format(new Date(), "yyyyMMddHHmmss") + "_"
					+ inputFile.getName()));
		}
		inputFile.renameTo(new File(destFolder + File.separator
				+ inputFile.getName()));
	}

	public void reCalSellerUserData(long sellerId) {
		String sql = " select min(paydate) min,max(paydate) max from t_taobao_trade where sellerid="
				+ sellerId;
		List list = this.commonService.queryListBySql(sql);
		if (CollectionUtils.isEmpty(list))
			return;
		Map map = (Map) list.get(0);
		Date min = (Date) map.get("min");
		Date max = (Date) map.get("max");
		if (min == null)
			return;
		preCalculateEngineService.calculateHisdataBySellerIdDates(sellerId, min, max);
	}

	public void doOther(long sellerId, String uuid) throws Exception {
		facadeService.sendSellerDataRcalJob(sellerId,uuid);
	}
}
