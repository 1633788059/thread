package com.wangjubao.app.others.service.buyerRepay.impl;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.wangjubao.app.others.service.buyerRepay.BuyerRepayReportService;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dao.BuyerDayDao;
import com.wangjubao.dolphin.biz.dao.BuyerRepayDao;
import com.wangjubao.dolphin.biz.dao.DolphinItemDao;
import com.wangjubao.dolphin.biz.dao.OrderDao;
import com.wangjubao.dolphin.biz.dao.SellerDao;
import com.wangjubao.dolphin.biz.dao.SellerProductSubscribeDao;
import com.wangjubao.dolphin.biz.dao.TradeDao;
import com.wangjubao.dolphin.biz.model.BuyerRepayDo;
import com.wangjubao.dolphin.biz.model.ItemDo;
import com.wangjubao.dolphin.biz.model.OrderDo;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.common.lang.Paginator;
import com.wangjubao.dolphin.common.util.Day;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.date.DateUtils;

@Service("buyerRepayReportService")
public class BuyerRepayReportServiceImpl implements BuyerRepayReportService {

    private static Logger             logger = LoggerFactory.getLogger("histroyimport");

    private static Gson               gson   = new Gson();

    @Autowired
    private BuyerDayDao               buyerDaydao;
    @Autowired
    private OrderDao                  orderDao;
    @Autowired
    private BuyerRepayDao             buyerRepayDao;
    @Autowired
    private SellerDao                 sellerDao;
    @Autowired
    private SellerProductSubscribeDao sellerProductSubscribeDao;
    @Autowired
    private DolphinItemDao            dolphinItemDao;
    @Autowired
    private TradeDao                  tradeDao;

    /**
     * 按商品id和买家id来计算并插入表中,按天得出所有交易 按人获取每人每天的详情
     */

    @Override
    public void anaBuyerRepayReport(Long sellerId, Date startDate) throws ParseException {
        //获取最近一次计算成功记录时间
    	//获取t_trade最近一天记录时间，对比t_seller的consumeTime。
        Date minTime = tradeDao.listForMinCreatedBySellerId(sellerId);
        SellerDo sellerDo = this.sellerDao.load(sellerId);
        Date consumeTime = sellerDo.getConsumeRepayDay();
        if (consumeTime != null && consumeTime.after(minTime)) {
        	//如果为1970-01-01代表需要重算
            if(Day.isSameDay(new Day(consumeTime), "1970-01-01")){
                this.buyerRepayDao.removeErrorLog(sellerId);
                //清算后要重新算，必须重设置一个值
                SellerDo reset = new SellerDo();
                reset.setConsumeRepayDay(null);
                reset.setId(sellerId);
                this.sellerDao.updateBuyerRepayTime(reset);
            }else{
                minTime = consumeTime;
            }
        } else {
            //如果seller表中这个字段为空代表要么需要重算要么就没数据，所以直接将该卖家数据清空
            this.buyerRepayDao.removeErrorLog(sellerId);
            //清算后要重新算，必须重设置一个值
            SellerDo reset = new SellerDo();
            reset.setConsumeRepayDay(null);
            reset.setId(sellerId);
            this.sellerDao.updateBuyerRepayTime(reset);
        }
        if (consumeTime == null && minTime == null) {
            logger.info(String.format("{ %s } the seller buyers is null . ", sellerId));
            return;
        }
        if (startDate == null || startDate.before(minTime)) {
            startDate = minTime;
        }

        //否则以大的时间作为开始时间，以昨天作为结束时间
        Date endDate = DateUtils.nextDays(new Date(), -1);

        if (logger.isInfoEnabled()) {
            logger.info(String.format("execute seller :{ %s } during【%s】", sellerId,
                    DateUtils.formatDate(startDate)));
        }
        for (Date date = startDate; !date.after(endDate);) {
            Day day = new Day(date);
            //历史订单，重算，会reset为1970-01-01，为防止这种情况则》
            SellerDo sellerDoCheck = this.sellerDao.load(sellerId);
            PageQuery pageQueryCheck = new PageQuery(0, 1);
            BuyerRepayDo buyerRepayDoCheck = new BuyerRepayDo();
            buyerRepayDoCheck.setSellerId(sellerId);
//            PageList<BuyerRepayDo> resultDoCheck  = this.buyerRepayDao.listBuyerRepay(buyerRepayDoCheck, pageQueryCheck);
            if(sellerDoCheck!=null&&sellerDoCheck.getConsumeRepayDay()!=null&&Day.isSameDay(new Day(sellerDoCheck.getConsumeRepayDay()), "1970-01-01")){
               
            }else{
                calculateData(sellerId, day);
            }
            date = DateUtils.nextDays(date, 1);
        }
    }

    private void calculateData(Long sellerId, Day day) {
        PageQuery pageQuery = new PageQuery(0, 4000);
        Date dete = day.getDayAsDate();
        Date begin = DateUtils.startOfDate(dete);
        Date end = DateUtils.endOfDate(dete);
        
        while (true) {

            PageList<OrderDo> resultDo = this.orderDao.listItemListByday(sellerId, begin, end,
                    pageQuery);

            if (resultDo == null) {
                break;
            }
            for (OrderDo s : resultDo) {
//                PageQuery pageQueryBuyer = new PageQuery(0, 4000);
                	//首先通过t_order 表获取某一天的所有商品信息，在通过遍历这些商品信息获取购买这些商品的会员信息
//                    PageList<OrderDo> resultBuyerDo = this.orderDao.listBuyerListByday(sellerId,
//                            s.getSourceItemId(), begin, end, pageQueryBuyer);
                if(s.getSourceItemId()!=null)
                {
                	List<OrderDo> resultBuyerDo=this.orderDao.listItemBuyerDetailByday(sellerId,s.getSourceItemId(),null,begin,end);
                	//状态为1,4,5，6，7,9有效
                	Map<Long,List<OrderDo>> orderMap=new HashMap<Long,List<OrderDo>>();
                	for(OrderDo b : resultBuyerDo)
                	{
                		List<OrderDo> orderList;
                		if(b.getStatus()==1||b.getStatus()==4||b.getStatus()==5
                				||b.getStatus()==6||b.getStatus()==7||b.getStatus()==9)
                		{
                			if(orderMap.containsKey(b.getBuyerId()))
                			{
                				orderList=orderMap.get(b.getBuyerId());
                				orderList.add(b);
                				orderMap.put(b.getBuyerId(), orderList);
                			}
                			else
                			{
                				orderList=new ArrayList<OrderDo>();
                				orderList.add(b);
                				orderMap.put(b.getBuyerId(), orderList);
                			}
                		}
                	}
                	Iterator<Map.Entry<Long, List<OrderDo>>> it = orderMap.entrySet().iterator();
                	while(it.hasNext())
                	{
                		  BuyerRepayDo buyerRepayDo = new BuyerRepayDo();
                          buyerRepayDo.setPayment(BigDecimal.ZERO);
                          Map.Entry<Long, List<OrderDo>> entry = it.next();
                          List<OrderDo> consumeList = entry.getValue();
                          for (int i = 0; i < consumeList.size(); i++) {
                              OrderDo consume = consumeList.get(i);
                              // 在遍历这些会员通过t_order 获取这些会员针对这些商品的消费金额
                              buyerRepayDo.setPayment(buyerRepayDo.getPayment().add(
                                      consume.getPayment()));
                          }
                          //并算出平均回购周期记录在t_buyer_repay表里面。
                          if (consumeList != null && consumeList.size() > 0) {
                              OrderDo con = consumeList.get(0);
                              if(con.getSourceItemId()!=null&&con.getBuyerId()!=null){
                                  insertOrUpdateRepay(day, buyerRepayDo, con);
                              }
                	}
                    }
                }
            }
            Paginator paginator = resultDo.getPaginator();
            if (paginator.getNextPage() == paginator.getPage()) {
                break;
            }
            pageQuery.increasePageNum();
        }
        SellerDo record = new SellerDo();
        record.setId(sellerId);
        Date d = day.getDayAsDate();
        record.setConsumeRepayDay(d);
        //历史订单，重算，会reset为1970-01-01，为防止这种情况则》
        SellerDo sellerDoCheck = this.sellerDao.load(sellerId);
        PageQuery pageQueryCheck = new PageQuery(0, 1);
        BuyerRepayDo buyerRepayDoCheck = new BuyerRepayDo();
        buyerRepayDoCheck.setSellerId(sellerId);
//        PageList<BuyerRepayDo> resultDoCheck  = this.buyerRepayDao.listBuyerRepay(buyerRepayDoCheck, pageQueryCheck);
        if(sellerDoCheck!=null&&sellerDoCheck.getConsumeRepayDay()!=null&&Day.isSameDay(new Day(sellerDoCheck.getConsumeRepayDay()), "1970-01-01")){
           
        }else{
            this.sellerDao.updateBuyerRepayTime(record);
        }
    }

    /**
     * 计算每个买家对应好商品的回购周期，金额等 回购金额，比如一个买家一天买了n次则金额是n次的和，但次数只加1
     */

    private void insertOrUpdateRepay(Day day, BuyerRepayDo buyerRepayDo, OrderDo con) {
        buyerRepayDo.setSellerId(con.getSellerId());
        buyerRepayDo.setSourceItemId(con.getSourceItemId());
        buyerRepayDo.setBuyerId(con.getBuyerId());
        buyerRepayDo.setBuyerNick(con.getBuyerNick());
        buyerRepayDo.setTitle(con.getTitle());
        buyerRepayDo.setOuterId(con.getOuterId());
        buyerRepayDo.setGmtModified(day.getDayAsDate());
        buyerRepayDo.setStatus(2);
        ItemDo itm = getCids(con.getSourceItemId(), con.getSellerId());
        if (itm != null) {
            buyerRepayDo.setCid(itm.getCid());

        }
        BuyerRepayDo buyerRepayDo2 = this.buyerRepayDao.loadBuyerRepay(buyerRepayDo);
        if (buyerRepayDo2 == null) {

            buyerRepayDo.setPurchaseTimes(1);
            buyerRepayDo.setBackPayDay(0);
            //本次购买时间
            this.buyerRepayDao.create(buyerRepayDo);
//            logger.info(String.format("execute seller :{ %s } nick:【%s】during【%s】create success",
//                    con.getSellerId(), con.getBuyerNick(), DateUtils.formatDate(day.getDayAsDate())));
        } else {
            //为了防止重复，即今天的算昨天的数据，或者说已经算过了再次算

            Integer backPayday = buyerRepayDo2.getBackPayDay();
            Integer purchaseTimes = buyerRepayDo2.getPurchaseTimes();
            //上次购买时间与本次购买时间相减
            Integer diff = 0;
            diff = DateUtils.getDays(buyerRepayDo2.getGmtModified(), day.getDayAsDate());
            if (diff == 0) {
                //先去掉之前的，在获取最新的，购买次数不便，购买周期不便，变的只是购买金额
            } else {
                //计算每个买家对应好商品的回购周期，金额等 回购金额，比如一个买家一天买了n次则金额是n次的和，但次数只加1
                buyerRepayDo.setPurchaseTimes(buyerRepayDo2.getPurchaseTimes() + 1);
                int backPaydayNew = (backPayday.intValue() * (purchaseTimes.intValue() - 1) + diff
                        .intValue()) / purchaseTimes;
                buyerRepayDo.setBackPayDay(backPaydayNew);
                buyerRepayDo.setPayment(buyerRepayDo2.getPayment().add(buyerRepayDo.getPayment()));
                this.buyerRepayDao.updateRepay(buyerRepayDo);
//                logger.info(String.format(
//                        "execute seller :{ %s } nick:【%s】during【%s】update success",
//                        con.getSellerId(), con.getBuyerNick(),
//                        DateUtils.formatDate(day.getDayAsDate())));
            }

        }
    }

    /**
     * 通过itemid调接口获取cid和parentcid
     */
    private ItemDo getCids(String sourceItemId, Long sellerId) {
        ItemDo item = new ItemDo();
        /*
         * 掉淘宝接口 List<SellerProductSubscribeDo> products =
         * sellerProductSubscribeDao .listBySellerId(sellerId); if (products ==
         * null || products.isEmpty()) { return null; } for
         * (SellerProductSubscribeDo sellerProductSubscribeDo : products) { if
         * (sellerProductSubscribeDo.getProductId().intValue() ==
         * ProductType.CRM .getValue()) { if
         * (StringUtils.isBlank(sellerProductSubscribeDo.getParameter())) {
         * return null; } AppParameterTaobao sellerTaobaoParameter =
         * gson.fromJson( sellerProductSubscribeDo.getParameter(),
         * AppParameterTaobao.class); if (sellerTaobaoParameter == null) {
         * return null; } String appkey = sellerTaobaoParameter.getAppkey();
         * String secret = sellerTaobaoParameter.getSecret(); String sessionKey
         * = sellerTaobaoParameter.getAccessToken(); TaobaoClient client=new
         * DefaultTaobaoClient("http://gw.api.taobao.com/router/rest", appkey,
         * secret, "json"); ItemGetRequest req=new ItemGetRequest();
         * req.setFields("num_iid,title,price,cid,seller_cids");
         * req.setNumIid(Long.valueOf(sourceItemId)); ItemGetResponse response =
         * null; try { response = client.execute(req,sessionKey); } catch
         * (ApiException e) { // TODO Auto-generated catch block
         * e.printStackTrace(); } if(response.isSuccess()){
         * item=response.getItem(); ItemcatsGetRequest reqI=new
         * ItemcatsGetRequest();
         * reqI.setFields("cid,parent_cid,name,is_parent");
         * reqI.setCids(item.getCid()+""); ItemcatsGetResponse res = null; try {
         * res = client.execute(reqI); } catch (ApiException e) { // TODO
         * Auto-generated catch block e.printStackTrace(); }
         * if(res.isSuccess()&&res.getItemCats().size()>0){
         * item.setAfterSaleId(res.getItemCats().get(0).getParentCid()); }
         * }else{
         * logger.info(String.format("execute seller :{ %s } errorFromTaobao【%s】"
         * , sellerId, response.getSubMsg()+response.getMsg())); } } }
         */
        //查t_item表
        item = this.dolphinItemDao.loadBySourceId(sellerId, sourceItemId);
        return item;
    }

    public static void main(String[] args) {
        int i = 2;
        int b = 2 / 3;
        System.out.println(b);
    }
}
