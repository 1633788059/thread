package com.wangjubao.app.others.service;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.springframework.util.CollectionUtils;

import com.wangjubao.core.domain.buyer.SellerUser;
import com.wangjubao.core.service.buyer.BuyerAddupVO;
import com.wangjubao.framework.util.DateUtil;

public interface IHistoryTradeImportService {
	
	/**
	 * 导入历史交易
	 * @param sellerId 卖家id
	 * @param basePath 根路径，如：F:\\test
	 * 然后会根据F:\\test\sellerId\order\*.csv,处理完后文件后转到
	 * F:\\test\sellerId\orderok\*.csv
	 * @return
	 */
	//public String importTrade(long sellerId,String basePath);
	/**
	 * 更新处理完成表示
	 * @param uuid
	 */
	//public void updateSuccess(String uuid,int oldstatus,int status,Long sellerId);
	/**
	 * 重算某个卖家的所有交易数据:t_seller_day,t_seller_user_day
	 * @param sellerId
	 */
	//public void reCalSellerUserData(long sellerId);
	/**
	 * 
	 * @param files
	 * @param sellerId
	 * @param basePath
	 * @return
	 */
	//public String importTrade(File[] files,Long sellerId,String basePath,Long id);
	
	
	
	/**
	 * 返回待处理的文件列表
	 * 
	 * @param sellerId
	 * @param basePath
	 * @return
	 */
	public File[] queryFileArray(Long sellerId, String basePath) ;

	public void updateSuccess(String uuid, int oldstatus, int status,Long sellerId) ;

	public void saveLog(long sellerId, String fileName, int status,
			String remark, String uuid) ;
	
	public void saveLog(long sellerId, String fileName, int status,
			String remark, long tid, String uuid) ;

	public boolean isExistsTrade(String tradeSourceId,Long sellerId) ;
	
	public Long isExistsSellerUser(long sellerId, String buyernick);

	public void moveToOkFolder(File inputFile, String destFolder) ;

	public void reCalSellerUserData(long sellerId) ;

	public void doOther(long sellerId, String uuid) throws Exception ;
	
	public String buildFilePath(long sellerId, String basePath) ;

	public String buildOkFilePath(long sellerId, String basePath) ;
}
