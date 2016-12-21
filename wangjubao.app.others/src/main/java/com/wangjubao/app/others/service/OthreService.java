package com.wangjubao.app.others.service;

/**
 * @author ckex created 2013-8-6 - 下午6:01:07 OthreService.java
 * @explain -
 */
public interface OthreService {

    /**
     * @param sellerId
     * 重算賣家的地址信息
     */
    void recountBuyerAddress(Long sellerId);
}
