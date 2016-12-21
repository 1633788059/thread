/**
 * 
 */
package com.wangjubao.app.other.data.clean.service;

import java.util.List;

import com.wangjubao.dolphin.biz.model.SellerDo;

/**
 * @author ckex created 2013-9-24 - 下午5:18:58 DataCleanService.java
 * @explain -
 */
public interface DataCleanService {

    void execute(SellerDo sellerDo);

    void execute(List<SellerDo> list);

    void cleanDataBySourceKey(List<SellerDo> sellerList);

}
