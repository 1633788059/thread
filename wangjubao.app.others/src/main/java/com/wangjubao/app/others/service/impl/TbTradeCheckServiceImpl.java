package com.wangjubao.app.others.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.taobao.api.TaobaoClient;
import com.taobao.api.domain.Trade;
import com.taobao.api.request.TradesSoldGetRequest;
import com.taobao.api.response.TradesSoldGetResponse;
import com.wangjubao.app.others.service.ITradeCheckService;
import com.wangjubao.core.dao.ICommonDao;
import com.wangjubao.core.domain.seller.TaobaoUser;
import com.wangjubao.core.service.syn.IClientService;
import com.wangjubao.core.service.syn.ITradeCheckLogicService;
import com.wangjubao.core.util.Constant;
import com.wangjubao.core.util.SynCache;

/**
 * 实现和taobao平台的交易数据校验核对
 * @author john_huang
 *
 */
public class TbTradeCheckServiceImpl implements ITradeCheckService {
	private transient final static Logger logger = Logger.getLogger("others");
	private ICommonDao commonService;

	ITradeCheckLogicService tradeCheckLogicService;

	private IClientService clientService;
	private 	org.springframework.amqp.rabbit.core.RabbitTemplate firstTradeCalculateRabbitTemplate;
//	@Autowired
//	@Qualifier("tBOrderDetailSynPipe")
//	private OrderBusinessInterfacePipe tBOrderDetailSynPipe;
//
//	@Autowired
//	@Qualifier("sellerUserDayAddupPipe")
//	private OrderBusinessInterfacePipe sellerUserDayAddupPipe;
//
//	@Autowired
//	@Qualifier("orderNickCalPipe")
//	private OrderBusinessInterfacePipe orderNickCalPipe;

	@Override
	public void onCheckAndAdjustByTradeCreateTime(Long sellerId, Date beginDate, Date endDate) throws Exception {
		long dbCount = tradeCheckLogicService.getTraeCountFromDb(sellerId, beginDate, endDate, SynCache.getSession(sellerId));
		long taobaoCount = tradeCheckLogicService.getTradeCountFromTaobao(sellerId, beginDate, endDate, SynCache.getSession(sellerId));

		if (dbCount == taobaoCount)
			return;

		// 如果不相等，则需要循环取tb的交易id和到系统进行校验，如果不存在，则需要重新取一次详细信息
		this.compareDiff(sellerId, beginDate, endDate, SynCache.getSession(sellerId));
	}

	private void compareDiff(Long sellerId, Date begin, Date end, String topSession) throws Exception {
		TradesSoldGetRequest request = this.buildReqByCreated(begin, end);
		TaobaoClient client = clientService.getTaobaoClient(sellerId);
		TradesSoldGetResponse res = this.executeQuery(client, request, topSession);
		List<Trade> tradeList = res.getTrades();

		// 比较taobao的交易id在系统是否存在，如果不存在则重新同步
		this.compareWithDbTrade(tradeList, sellerId);

		long repeat = this.getPageNum(res.getTotalResults());

		if (repeat > 0) {
			for (long i = 1; i <= repeat; i++) {
				request.setPageNo(i + 1);
				res = this.executeQuery(client, request, topSession);
				// 比较taobao的交易id在系统是否存在，如果不存在则重新同步
				this.compareWithDbTrade(res.getTrades(), sellerId);
			}
		}
	}

	// 比较taobao的交易id在系统是否存在，如果不存在则重新同步
	private void compareWithDbTrade(List<Trade> tradeList, long sellerId) {

		if (CollectionUtils.isEmpty(tradeList))
			return;
		StringBuilder sb = new StringBuilder();
		for (Trade trade : tradeList) {
			sb.append("'").append(trade.getTid()).append("',");
		}
		sb.append("-1");
		String sql = "select tradeSourceId from t_taobao_trade where sellerId=" + sellerId + " and shopType=" + TaobaoUser.SHOP_TYPE_TAOBAO + " and "
				+ " tradeSourceId in(" + sb.toString() + ")";
		List<Map> mapList = this.commonService.queryListBySql(sql);
		if (mapList != null && mapList.size() == tradeList.size())
			return;

		List<Long> diffList = this.calculateMissingTradeIdList(tradeList, mapList);
		if (diffList.size() == 0)
			return;
		logger.info("sellerId="+sellerId+" 遗漏如下交易数据=["+Arrays.toString(diffList.toArray(new Long[diffList.size()]))+"]");
		// 从新从taobao同步丢失的交易信息
		this.reSynTradeDataFromTb(diffList, sellerId);
	}

	private void reSynTradeDataFromTb(List<Long> tradeList, Long sellerId) {
		//修改成发mq
//		for (Long id : tradeList) {
//			PiperContext context = new PiperContext();
//			BuyerAddupVO vo = new BuyerAddupVO();
//			vo.setSellerId(sellerId);
//			vo.setTid(id);
//			context.setAddupVO(vo);
//			try {
//				tBOrderDetailSynPipe.doPipe(context);
//				sellerUserDayAddupPipe.doPipe(context);
//				orderNickCalPipe.doPipe(context);
//			} catch (Exception ex) {
//				logger.info("", ex);
//			}
//		}
	}

	// 比较交易id的差异
	private List<Long> calculateMissingTradeIdList(List<Trade> tradeList, List<Map> mapList) {
		Map<Long, Long> idMap = new HashMap<Long, Long>();
		for (Map map : mapList) {
			idMap.put(Long.parseLong(map.get("tradeSourceId").toString()), Long.parseLong(map.get("tradeSourceId").toString()));
		}
		List<Long> result = new ArrayList<Long>();

		for (Trade trade : tradeList) {
			if (idMap.get(trade.getTid()) == null)
				result.add(trade.getTid());
		}

		return result;
	}

	private TradesSoldGetResponse executeQuery(TaobaoClient client, TradesSoldGetRequest request, String topSession) throws Exception {
		TradesSoldGetResponse res;
		int errorNum = 0;
		while (true) {
			try {
				res = client.execute(request, topSession);

				if (res == null || !res.isSuccess()) {
					errorNum++;
					if (errorNum > 4) {
						throw new Exception(res.getBody());
					}
					continue;
				}
				break;
			} catch (Exception ex) {
				if (errorNum > 4)
					throw new Exception(ex);
				continue;
			}
		}
		return res;
	}

	protected long getPageNum(long totalCount) {
		long repeat = 0;
		if ((totalCount - Constant.pageSize) % Constant.pageSize > 0) {
			repeat = (totalCount - Constant.pageSize) / Constant.pageSize + 1;
		} else {
			repeat = (totalCount - Constant.pageSize) / Constant.pageSize;
		}
		return repeat;
	}

	private TradesSoldGetRequest buildReqByCreated(Date begin, Date end) {
		TradesSoldGetRequest req = new TradesSoldGetRequest();
		req.setStartCreated(begin);
		if (end != null)
			req.setEndCreated(end);

		req.setPageNo(1L);
		req.setPageSize((long) Constant.pageSize);
		req.setFields("tid");
		return req;
	}

	public ICommonDao getCommonService() {
		return commonService;
	}

	public void setCommonService(ICommonDao commonService) {
		this.commonService = commonService;
	}

	public ITradeCheckLogicService getTradeCheckLogicService() {
		return tradeCheckLogicService;
	}

	public void setTradeCheckLogicService(ITradeCheckLogicService tradeCheckLogicService) {
		this.tradeCheckLogicService = tradeCheckLogicService;
	}

	public IClientService getClientService() {
		return clientService;
	}

	public void setClientService(IClientService clientService) {
		this.clientService = clientService;
	}

	public org.springframework.amqp.rabbit.core.RabbitTemplate getFirstTradeCalculateRabbitTemplate() {
		return firstTradeCalculateRabbitTemplate;
	}

	public void setFirstTradeCalculateRabbitTemplate(org.springframework.amqp.rabbit.core.RabbitTemplate firstTradeCalculateRabbitTemplate) {
		this.firstTradeCalculateRabbitTemplate = firstTradeCalculateRabbitTemplate;
	}
	
	
}
