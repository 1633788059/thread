package com.wangjubao.app.others.service;

public interface IHisTradeCsvImportService {
	public static final String PLATFORM_TB="TB";
	/**
	 * 卖家历史交易数据导入
	 * @param sellerId	卖家id
	 * @param platform	平台类型
	 * @param basePath  根路径
	 * @return	导入的文件名称数组
	 */
	public String[] importHisTrade(Long sellerId,String platform,String basePath);
	
	public void jobHisImportTrade(Long sellerId,String basePath);
}
