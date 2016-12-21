package com.wangjubao.app.others.service.downcenter;

import com.wangjubao.dolphin.biz.model.SysSyntaskDo;


public interface DownCenterService {
	/**
	 * 初始化
	 */
	public void init(SysSyntaskDo sysSyntaskDo);
	
	/**
	 * 导出或者上传
	 */
	public void job(SysSyntaskDo sysSyntaskDo);
	
	/**
	 * 清除
	 */
	public void clean(SysSyntaskDo sysSyntaskDo);
}
