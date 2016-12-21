package com.wangjubao.app.others.service.promotionsync;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.alibaba.fastjson.JSONObject;
import com.taobao.api.domain.PromotionDetail;
import com.wangjubao.dolphin.biz.common.constant.SellerType;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dao.PromotionDetailDao;
import com.wangjubao.dolphin.biz.dao.TradeDao;
import com.wangjubao.dolphin.biz.model.PromotionDetailDo;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.biz.model.TradeDo;
import com.wangjubao.dolphin.biz.service.SellerService;
import com.wangjubao.dolphin.common.util.StrUtils;
import com.wangjubao.dolphin.common.util.date.DateUtils;

@Service
public class PromotionSyncService {
	
	static final Logger                               logger       = Logger.getLogger("promotionsync");

	@Autowired
	private SellerService sellerService;
	
	@Autowired
	private TradeDao tradeDao;
	
	@Autowired
	private PromotionDetailDao promotionDetailDao;
	
	public void sync(String sellerNick) {
//		logger.info("——-----------------------进入sync。。。");
		SellerDo sellerDo = this.loadCacheByNick(sellerNick);
		if (sellerDo==null) {
			return;
		}
		Date startTime = DateUtils.formartDate(2015,1,1,0,0,0);
		Date endTime = DateUtils.nextDays(new Date(), -1);
		if (sellerDo.getPromotionDetailSync()!=null && sellerDo.getPromotionDetailSync().after(startTime)) {
			startTime = sellerDo.getPromotionDetailSync();
		}else{
			logger.info("最后同步时间为空或者小于");
		}
		
		//如果是当天每5分钟拉一次,如果不是当前天，每一天拉一次
        for (Date startDate = startTime; DateUtils.diffSecond(new Date(), startDate)>10*60;) {
        	Date endDate = endTime;
        	if (DateUtils.diffDays(startDate,new Date())==0) {
        	////  ？？是不是5分钟太短了？
				endDate = DateUtils.nextMinute(startDate, 5);
			}else {
				endDate = DateUtils.nextDays(startDate, 1);
			}
        	if (endDate.after(new Date())) {
				endDate = new Date();
			}
        	
        	long start = System.currentTimeMillis();
            listTradeDosByGmtModified(sellerDo,startDate,endDate);
//            logger.info("---------------------listTradeDosByGmtModified耗时"+
//                    (System.currentTimeMillis()-start)/1000.00+"秒    "+
//                   "sellerId="+sellerDo.getId()+"    sellerNick="+sellerDo.getNick()
//               );
            
            startDate = endDate;
        }
		
		
	}
	
	
	/**
	 * 获取指定时间段的订单信息并解析
	 * @param sellerDo
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	private List<TradeDo> listTradeDosByGmtModified(SellerDo sellerDo,Date startDate,Date endDate){
		List<TradeDo> tradeDos = new ArrayList<TradeDo>();
		PageQuery pageQuery = new PageQuery(1,200);
		while (true) {
			List<TradeDo> tradeDos2 = this.tradeDao.listPageByGmtModified(sellerDo.getId(),startDate,endDate,pageQuery);
			if (tradeDos2==null || tradeDos2.isEmpty()) {
				sellerDo.setPromotionDetailSync(endDate);
				this.sellerService.update(sellerDo);
				break;
			}
			//解析订单信息
			for (TradeDo tradeDo : tradeDos2) {
				analysisTrade(sellerDo, tradeDo);
			}
			SellerDo sellerDo2 = new SellerDo();
			sellerDo2.setId(sellerDo.getId());
			sellerDo2.setPromotionDetailSync(sellerDo.getPromotionDetailSync());
			this.sellerService.update(sellerDo2);
			pageQuery.increasePageNum();
		}
		return tradeDos;
	}

	/**
	 * 解析订单信息并插入数据库，并更新t_seller的promotion_detail_sync
	 * @param sellerDo
	 * @param tradeDo
	 */
	private void analysisTrade(SellerDo sellerDo, TradeDo tradeDo) {
		if (StrUtils.isNotEmpty(tradeDo.getPromotionDetails())) {
			List<PromotionDetailDo> promotionDetailDos = setPromotionDetailByTrade(sellerDo,tradeDo);
			if (promotionDetailDos==null) {
				//解析失败并记录数据库
				createFailLog(sellerDo, tradeDo);
				
			}else {
				//以订单为单位，有则删除无责批量插入,
				try {
//					this.promotionDetailDao.deleteByTid(sellerDo.getId(), Long.valueOf(tradeDo.getSourceId()));
					this.promotionDetailDao.createBatch(sellerDo.getId(), promotionDetailDos);
				} catch (Exception e) {
					logger.info(String.format("[%s](%s)[%s]转换优惠信息失败，错误信息【%s】", sellerDo.getId(),sellerDo.getNick(),tradeDo.getSourceId(),StrUtils.showError(e)));
					createFailLog(sellerDo, tradeDo);
				}
				
			}
		}
		if(tradeDo.getGmtModified().after(sellerDo.getPromotionDetailSync())){
			sellerDo.setPromotionDetailSync(tradeDo.getGmtModified());
		}
		
		
	}


	/**
	 * 插入失败日志
	 * @param sellerDo
	 * @param tradeDo
	 */
	private void createFailLog(SellerDo sellerDo, TradeDo tradeDo) {
		PromotionDetailDo promotionDetailFailDo = new PromotionDetailDo();
		promotionDetailFailDo.setSellerId(sellerDo.getId());
		promotionDetailFailDo.setSourceTradeId(Long.valueOf(tradeDo.getSourceId()));
		promotionDetailFailDo.setStatus(3);
		this.promotionDetailDao.create(promotionDetailFailDo);
	}


	/**
	 * 根据昵称获取卖家信息
	 * @param sellerNick
	 * @return
	 */
	 private SellerDo loadCacheByNick(String sellerNick)
	    {
		 	SellerDo seller = new SellerDo();
		 	seller.setNick(sellerNick);
	        List<SellerDo> sellers = sellerService.loadSellerByNick(seller);
	        if (sellers != null && !sellers.isEmpty())
	        {
	            for (Iterator<SellerDo> iterator = sellers.iterator(); iterator.hasNext();)
	            {
	                SellerDo sellerDo = iterator.next();

	                Integer sellerType = sellerDo.getSellerType();
	                boolean sellerTypeIsNull = sellerType == null;
	                if (sellerTypeIsNull)
	                {
	                    continue;
	                }

	                boolean sellerTypeIsInvalid = sellerType.intValue() == SellerType.TAOBAO
	                        || sellerDo.getSellerType().intValue() == SellerType.TMALL
	                        || sellerDo.getSellerType().intValue() == SellerType.QIANNIU;

	                if (sellerTypeIsInvalid)
	                {
	                    return sellerDo;
	                }
	            }
	        }
	        return null;
	    }
	 
	 /**
	  * 根据优惠信息json获取对应的javabean
	  * @param promotionDetailJson
	  * @return
	  */
	public List<PromotionDetailDo> setPromotionDetailByTrade(SellerDo sellerDo,TradeDo tradeDo){
		List<PromotionDetailDo> promotionDetailDos = new ArrayList<PromotionDetailDo>();
		String promotionDetailJson = tradeDo.getPromotionDetails();
		if (StrUtils.isEmpty(promotionDetailJson )) {
			return null;
		}
		try {
			List<PromotionDetail> promotionDetails = (List<PromotionDetail>)JSONObject.parseArray(promotionDetailJson,PromotionDetail.class);
			for (PromotionDetail promotionDetail : promotionDetails) {
				PromotionDetailDo promotionDetailDo = new PromotionDetailDo();
				promotionDetailDo.setSellerId(sellerDo.getId());
				promotionDetailDo.setSourceTradeId(promotionDetail.getId());
				promotionDetailDo.setStatus(2);
				promotionDetailDo.setDiscountFee(StrUtils.isNotEmpty(promotionDetail.getDiscountFee())?new BigDecimal(promotionDetail.getDiscountFee()):BigDecimal.ZERO);
				if (StrUtils.isNotEmpty(promotionDetail.getGiftItemId()) && promotionDetail.getGiftItemId()!="null" && StrUtils.isNumeric(promotionDetail.getGiftItemId())) {
					promotionDetailDo.setGiftItemId(Long.valueOf(promotionDetail.getGiftItemId()));
				}
				promotionDetailDo.setGiftItemName(promotionDetail.getGiftItemName());
				
				if (StrUtils.isNotEmpty(promotionDetail.getGiftItemNum()) && promotionDetail.getGiftItemNum()!="null") {
					promotionDetailDo.setGiftItemNum(Long.valueOf(promotionDetail.getGiftItemNum()));
				}
				
				promotionDetailDo.setPromotionId(promotionDetail.getPromotionId());
				promotionDetailDo.setPromotionDesc(promotionDetail.getPromotionDesc());
				promotionDetailDo.setPromotionName(promotionDetail.getPromotionName());
				promotionDetailDo.setBuyerNick(tradeDo.getBuyerNick());
				//营销工具id-优惠活动id_优惠详情id，如mjs-123024_211143）还有Tmall$tspAll-54611413这种情况。
				if (StrUtils.isNotEmpty(promotionDetail.getPromotionId())) {
					String[] detail = promotionDetail.getPromotionId().split("-");
					String toolId = detail[0];
					String[] actIdStrings = detail[1].split("_");
					String actId = actIdStrings[0];
					String detailId = actIdStrings.length>1?actIdStrings[1]:"";
					promotionDetailDo.setPromotionActId(actId);
					promotionDetailDo.setPromotionDetailId(detailId);
					promotionDetailDo.setPromotionToolId(toolId);
 				}
				
				promotionDetailDos.add(promotionDetailDo);
			}
		} catch (Exception e) {
			logger.info(String.format("[%s](%s)转换优惠信息失败订单【%s】，错误信息【%s】", sellerDo.getId(),sellerDo.getNick(),tradeDo.getSourceId(),StrUtils.showError(e)));
		}
		
		return promotionDetailDos;
				
	}


	/**
	 * 修复线程
	 */
	public void fix() {
		// TODO Auto-generated method stub
		
	}
}

