package com.wangjubao.app.others.service.itemCatSync;


public interface ItemCatSyncService {
	
	void syncItemCat();

    /**
     * 
     */
    void init();

    /**
     * 
     */
    void execute(Long parentCid);
}
