package com.wangjubao.app.others.service;

import java.util.Date;

/**
 * 核对并重新同步处理交易数据
 * @author john_huang
 *
 */
public interface ITradeCheckService {
	/**
	 * check系统的交易数据和淘宝等第三方平台的交易数据是否一致
	 * 如果不一致，则自动同步补全数据
	 * 
	 * 此方法是根据交易的创建日期进行check
	 * @param sellerId
	 * @throws Exception 
	 */
	public void onCheckAndAdjustByTradeCreateTime(Long sellerId,Date beginDate,Date endDate) throws Exception;
}
