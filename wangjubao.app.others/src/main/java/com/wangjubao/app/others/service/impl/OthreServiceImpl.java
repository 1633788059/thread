package com.wangjubao.app.others.service.impl;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.wangjubao.app.others.service.OthreService;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dal.extend.bo.AreaBo;
import com.wangjubao.dolphin.biz.dao.BuyerDao;
import com.wangjubao.dolphin.biz.dao.SellerDao;
import com.wangjubao.dolphin.biz.dao.TradeDao;
import com.wangjubao.dolphin.biz.model.AreaDo;
import com.wangjubao.dolphin.biz.model.BuyerDo;
import com.wangjubao.dolphin.biz.model.TradeDo;
import com.wangjubao.dolphin.common.lang.Paginator;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.StrUtils;

/**
 * @author ckex created 2013-8-6 - 下午6:01:31 OthreServiceImpl.java
 * @explain -
 */
@Service("othreService")
public class OthreServiceImpl implements OthreService {

    private transient final static Logger logger = LoggerFactory.getLogger("histroyimport");

    @Autowired
    private SellerDao                     sellerDao;

    @Autowired
    @Qualifier("dolphinBuyerDao")
    private BuyerDao                      buyerDao;

    @Autowired
    private TradeDao                      tradeDao;

    @Autowired
    private AreaBo                        areaBo;

    @Override
    public void recountBuyerAddress(Long sellerId) {

        long s = System.currentTimeMillis();
        logger.info(String.format("{ %s }", sellerId));
        PageQuery pageQuery = new PageQuery(0, 5000);
        while (true) {
            long start = System.currentTimeMillis();
            PageList<BuyerDo> list = buyerDao.listAllByInfo(sellerId, pageQuery);
            if (list == null || list.isEmpty()) {
                break;
            }
            int i = 0;
            for (Iterator<BuyerDo> it = list.iterator(); it.hasNext();) {
                BuyerDo buyer = it.next();
                if (isPass(buyer)) {
                    ++i;
                    continue;
                }
                recountAddress(buyer);
            }

            Paginator paginator = list.getPaginator();
            logger.info(String
                    .format(" syn buyer info : count( %s ),index( %s ), Csize( %s ), skip( %s ), times( %s ) -ms",
                            paginator.getItems(), pageQuery.getStartIndex(), list.size(), i,
                            (System.currentTimeMillis() - start)));
            if (paginator.getNextPage() == paginator.getPage()) {
                break;
            }
            pageQuery.increasePageNum();
        }
        logger.info(String.format("{ %s } execute times ( %s )-ms", sellerId,
                (System.currentTimeMillis() - s)));

    }

    private void recountAddress(BuyerDo buyer) {

        PageQuery pageQuery = new PageQuery(0, 5000);
        while (true) {
            PageList<TradeDo> list = tradeDao.listAll(buyer.getSellerId(), null, null,
                    buyer.getId(), buyer.getNick(), pageQuery);
            if (list == null || list.isEmpty()) {
                logger.error(String.format(" { %s }【%s】buyer trades is null . ",
                        buyer.getSellerId(), buyer.getNick()));
                break;
            }
            Collections.sort(list, new Comparator<TradeDo>() {

                @Override
                public int compare(TradeDo o1, TradeDo o2) {
                    return (int) (o2.getCreated().getTime() - o1.getCreated().getTime());
                }

            });
            boolean addressflag = Boolean.FALSE;
            boolean nameflag = Boolean.FALSE;
            boolean provinceflag = Boolean.FALSE;
            boolean cityflag = Boolean.FALSE;
            boolean districtflag = Boolean.FALSE;
            for (Iterator<TradeDo> iterator = list.iterator(); iterator.hasNext();) {
                TradeDo trade = iterator.next();
                if (!StrUtils.isNotEmpty(buyer.getAddress())
                        && StrUtils.isNotEmpty(trade.getReceiverAddress())) {
                    buyer.setAddress(StrUtils.subString(trade.getReceiverAddress(), 495));
                    addressflag = Boolean.TRUE;
                }

                if (!StrUtils.isNotEmpty(buyer.getRealName())
                        && StrUtils.isNotEmpty(trade.getReceiverName())) {
                    buyer.setRealName(trade.getReceiverName());
                    nameflag = Boolean.TRUE;
                }

                if (buyer.getProvince() == null && StrUtils.isNotEmpty(trade.getReceiverState())) {
                    AreaDo province = areaBo.getProvinceByName(trade.getReceiverState());
                    if (province != null) {
                        buyer.setProvince(province.getId());
                        provinceflag = Boolean.TRUE;
                    }
                }

                if (buyer.getCity() == null && StrUtils.isNotEmpty(trade.getReceiverCity())) {
                    AreaDo city = areaBo.getCityByName(trade.getReceiverCity());
                    if (city != null) {
                        buyer.setCity(city.getId());
                        cityflag = Boolean.TRUE;
                    }
                }

                if (buyer.getDistrict() == null && StrUtils.isNotEmpty(trade.getReceiverDistrict())) {
                    AreaDo district = areaBo.getDistrictByName(trade.getReceiverDistrict());
                    if (district != null) {
                        buyer.setDistrict(district.getId());
                        districtflag = Boolean.TRUE;
                    }
                }

                if (addressflag && nameflag && provinceflag && cityflag && districtflag) {
                    break;
                }
            }
            if (addressflag || nameflag || provinceflag || cityflag || districtflag) {
                buyerDao.update(buyer);
                logger.info(String.format("{ %s }【%s】update.", buyer.getSellerId(), buyer.getNick()));
            } else {
                if (!StrUtils.isNotEmpty(buyer.getAddress())) {
                    logger.error(String.format("{ %s }【%s】buyer info update fail XXXXXXXX",
                            buyer.getSellerId(), buyer.getNick()));
                }
            }

            Paginator paginator = list.getPaginator();
            if (paginator.getNextPage() == paginator.getPage()) {
                break;
            }
            pageQuery.increasePageNum();
        }

    }

    private boolean isPass(BuyerDo buyer) {
        if (buyer == null) {
            return Boolean.TRUE;
        } else if (!StrUtils.isNotEmpty(buyer.getRealName())
                || !StrUtils.isNotEmpty(buyer.getAddress()) || buyer.getProvince() == null
                || buyer.getCity() == null || buyer.getDistrict() == null) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

}
