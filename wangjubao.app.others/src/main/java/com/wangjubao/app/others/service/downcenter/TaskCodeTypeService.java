package com.wangjubao.app.others.service.downcenter;

public interface TaskCodeTypeService {

	/**
	 * 根据codeType获取对应的service
	 * @param codeType
	 * @return
	 */
	public DownCenterService getServiceByCodeType(String codeType);	
}
