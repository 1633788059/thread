package com.wangjubao.app.others.service.itemCatSync.impl;


import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.taobao.api.domain.SellerCat;
import com.taobao.api.response.SellercatsListGetResponse;
import com.wangjubao.app.others.service.itemCatSync.SellerCatSyncService;
import com.wangjubao.dolphin.biz.model.ItemCatDo;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.biz.service.ItemCatService;
import com.wangjubao.dolphin.biz.service.SellerSessionKeyService;
import com.wangjubao.dolphin.common.util.date.DateUtils;

@Service("sellerCatSyncService")
public class SellerCatSyncServiceImpl implements SellerCatSyncService {

	static final Logger                               logger       = Logger.getLogger("sellercats");
	
	@Autowired
	private SellerSessionKeyService sellerSessionKeyService;
	
	@Autowired
    private ItemCatService itemCatService;
	
	@Override
	public  void sync(String sellerNick) {
		
		logger.info(sellerNick+" ####################################### 拉类目列表信息启动..." + DateUtils.formatDate(DateUtils.now()));
		//调用淘宝接口
		SellerDo seller = this.sellerSessionKeyService.getSellerDoByNick(sellerNick);
		if (seller==null) {
			logger.info(String.format("%5s拉取类目失败，原因%s", sellerNick,
					"卖家返回空"));
		}else{
			
			SellercatsListGetResponse res = this.sellerSessionKeyService.getSellerCats(sellerNick);
			if (res!=null && res.isSuccess()) {
				if (res.getSellerCats() != null && !res.getSellerCats().isEmpty()) {
					
					for (SellerCat cats : res.getSellerCats()) {
						ItemCatDo itemCatDo = new ItemCatDo();
						itemCatDo.setType(cats.getType());
						itemCatDo.setSellerId(seller.getId());
						itemCatDo.setSourceId(cats.getCid().toString());
						itemCatDo.setParentCid(cats.getParentCid());
						itemCatDo.setName(cats.getName());
						itemCatDo.setPicUrl(cats.getPicUrl());
						if (cats.getSortOrder()!=null) {
							itemCatDo.setSortOrder(Integer.valueOf(cats.getSortOrder().toString()));
						}
						itemCatDo.setCreated(cats.getCreated());
						itemCatDo.setModified(cats.getModified());
						this.itemCatService.createOrUpdateCatDo(itemCatDo);
					}
					
				}
			} else {
				if (res!=null) {
					logger.info(String.format("%5s拉取类目失败，原因%s", sellerNick,
							res.getBody()));
				}else {
					logger.info(String.format("%5s拉取类目失败，原因%s", sellerNick,
							"信息返回空"));
				}
				
			}
		}
	}

}
