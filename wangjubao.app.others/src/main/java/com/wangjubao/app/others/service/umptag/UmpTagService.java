package com.wangjubao.app.others.service.umptag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.taobao.api.domain.MjsPromotion;
import com.taobao.api.domain.PromotionTag;
import com.wangjubao.core.domain.marketing.ActivityInfo;
import com.wangjubao.core.domain.marketing.ump.UmpAct;
import com.wangjubao.core.service.marketing.IActivityInfoService;
import com.wangjubao.core.util.BuyerSearchContext;
import com.wangjubao.dolphin.biz.common.constant.SellerType;
import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.dao.ActivityInfoDao;
import com.wangjubao.dolphin.biz.dao.BuyerDao;
import com.wangjubao.dolphin.biz.dao.UmpActivitydetailDao;
import com.wangjubao.dolphin.biz.model.ActivityInfoDo;
import com.wangjubao.dolphin.biz.model.BuyerDo;
import com.wangjubao.dolphin.biz.model.JsonActivityDetail;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.biz.model.UserDynamicGroupDo;
import com.wangjubao.dolphin.biz.service.BuyerImportService;
import com.wangjubao.dolphin.biz.service.DolphinBuyerSearcher;
import com.wangjubao.dolphin.biz.service.PromotionMjsService;
import com.wangjubao.dolphin.biz.service.SellerService;
import com.wangjubao.dolphin.biz.service.SellerSessionKeyService;
import com.wangjubao.dolphin.biz.service.UserDynamicGroupService;
import com.wangjubao.dolphin.common.util.PageList;
import com.wangjubao.dolphin.common.util.StrUtils;
import com.wangjubao.dolphin.common.util.date.DateUtils;

@Service
public class UmpTagService {
	
	static final Logger                               logger       = Logger.getLogger("umptag");

	 public static int            THREAD_NUM          = 3;
	 
	 private static Long TOOLID = 2802002l;//正式满就送app
	 
	@Autowired
	private SellerService sellerService;
	
	@Autowired
	private ActivityInfoDao activityInfoDao;
	
	@Autowired
	@Qualifier("dolphinBuyerDao")
	private BuyerDao buyerDao;
	
	static DolphinBuyerSearcher dolphinBuyerSearcher =null;
	
	@Autowired
	 private BuyerImportService   buyerImportService;
	
	@Autowired
	private UserDynamicGroupService       userDynamicGroupService;
	
	@Autowired
	private SellerSessionKeyService sellerSessionKeyService;
	
	@Autowired
	private PromotionMjsService promotionMjsService;
	
	
	@Autowired
	private UmpActivitydetailDao umpActivitydetailDao;
	
	@Autowired
	private IActivityInfoService activityInfoService;
	
	private DolphinBuyerSearcher getDolphinBuyerSearcher()
    {
        if (dolphinBuyerSearcher == null)
        {
            dolphinBuyerSearcher = (DolphinBuyerSearcher) BuyerSearchContext.getInstance().getBean("dolphinBuyerSearcher");
        }
        return dolphinBuyerSearcher;
    }
	
	
	public void sync(SellerDo sellerDo) {
/*		SellerDo sellerDo = this.loadCacheByNick(sellerNick);
		if (sellerDo==null) {
			return;
		}
*/
//		ActivityInfoDo activityInfoDo = new ActivityInfoDo();
//		activityInfoDo.setSellerId(sellerDo.getId());
//		activityInfoDo.setCurrentStatus(2);
//		activityInfoDo.setStatus(0);
//		activityInfoDo.setActivityType("131");
//		PageQuery pageQuery = new PageQuery(1,200);
//		while (true) {
//			PageList<ActivityInfoDo> activityInfoDos = this.activityInfoDao.listActivityInfoByPage(activityInfoDo, null, null, pageQuery);
//			if (activityInfoDos==null || activityInfoDos.size()==0) {
//				break;
//			}
//			for (ActivityInfoDo activityInfoDo2 : activityInfoDos) {
//				dowork(activityInfoDo2);
//			}
//			Paginator paginator = activityInfoDos.getPaginator();
//			 if (paginator.getNextPage() == paginator.getPage()) {
//                 break;
//             }
//			pageQuery.increasePageNum();
//		}
		
		//通过toolId（固定的）调用接口sellerSessionKeyService.getManjiuSongList获取满就减活动列表。
		List<String> manjiusongList = this.sellerSessionKeyService.getManjiuSongList(sellerDo.getId(), TOOLID);
		if(sellerDo.getId() == 835998948L){
			logger.info("可靠旗舰店满就减活动数量"+(manjiusongList==null?0:manjiusongList.size()));
		}
		if (manjiusongList!=null && !manjiusongList.isEmpty()) {
			logger.info("卖家["+sellerDo.getId()+sellerDo.getNick()+"]  满就减列表数量："+manjiusongList.size());
			for (String json : manjiusongList) {
				try {
					UmpAct umpact =new UmpAct();
		        	GsonBuilder builder = new GsonBuilder();
		        	Gson gson = builder.create();
		        	
		        	umpact=gson.fromJson(json, UmpAct.class);
		        	
		        	//对于已过期活动和一小时内过期活动不再执行补打标签
		        	Date now = new Date();
		        	Date endTime =  DateUtils.str2Date(umpact.getEndTime());
		        	endTime = DateUtils.nextHours(endTime, 1);
		    		if(now.after(endTime)){
		    			continue;
		    		}
		        	
		        	
		        	//以后在弄到dolphin里面算了
		        	 ActivityInfo act = new ActivityInfo();
	                 act.setSellerId(sellerDo.getId());
	                 act.setActivityType(ActivityInfo.ACTIVITY_UMP);
	                 act.setStatus(0);
	                 act.setActivityTitles(umpact.getActivityId()+"");
	                 act.setDesc("desc");
	                 act.setOrderField("createTime");
	                 List<ActivityInfo> actList= this.activityInfoService.queryActivityInfoListForUmp(act);
	                 if(actList!=null&&actList.size()>0){
	                     ActivityInfo actInfo = actList.get(0);
	                     ActivityInfoDo activityInfoDo = actInfo.infoToActivityInfoDo(actInfo);
	                     dowork(activityInfoDo);
	                 }
				} catch (Exception e) {
					logger.info(StrUtils.showError(e));
				}
				
	        	
			}
		}
		//超级满就送
		PageQuery pageQuery = new PageQuery(Integer.valueOf(1),Integer.valueOf(50));
		PageList<MjsPromotion> superMjsList = this.promotionMjsService.getPromotionMiscPageList(sellerDo.getId(), Long.valueOf(2), pageQuery);
		if (superMjsList !=null && !superMjsList.isEmpty()) {
			logger.info("卖家["+sellerDo.getId()+sellerDo.getNick()+"]  满就送列表数量："+superMjsList.size());
			for (MjsPromotion promotion : superMjsList) {
				//因为前段显示需要更详细丰满的信息，所以需要在掉接口获取详情
				promotion = this.promotionMjsService.getPromotionMjs(sellerDo.getId(), promotion.getActivityId());
				//对于已过期活动和一小时内过期活动不再执行补打标签
	        	Date now = new Date();
	        	Date endTime =  promotion.getEndTime();
	        	endTime = DateUtils.nextHours(endTime, 1);
	    		if(now.after(endTime)){
	    			continue;
	    		}
	        	 ActivityInfo act = new ActivityInfo();
                 act.setSellerId(sellerDo.getId());
                 act.setActivityType(ActivityInfo.ACTIVITY_UMP);
                 act.setStatus(0);
                 act.setActivityTitles(promotion.getActivityId()+"");
                 act.setDesc("desc");
                 act.setOrderField("createTime");
                 List<ActivityInfo> actList= this.activityInfoService.queryActivityInfoListForUmp(act);
                 if(actList!=null&&actList.size()>0){
                     ActivityInfo actInfo = actList.get(0);
                     ActivityInfoDo activityInfoDo = actInfo.infoToActivityInfoDo(actInfo);
                     dowork(activityInfoDo);
                 }
			}
		}else{
			if(sellerDo.getId() == 835998948L)
			logger.info("满就送superMjsList为空");
		}
	}

	/**
	 * 优惠券发送
	 * @param activityInfoDo2
	 */
	private void dowork(ActivityInfoDo act) {
		 // 获取发送列表
        
        Long tagId = JsonActivityDetail.activityDetailJsonStrToBean(act.getActivityDetailgsontStr()).getTagId();
        if (tagId == null) // 查询会员列表出错
        {
        } else
        {
	        PromotionTag tag = sellerSessionKeyService.getPromotionTag(tagId, act.getSellerId());
            if (null == tag || tag.getEndTime().before(new Date())) {
            	logger.info("标签过期 tagId:"+tagId+" [sellerId:"+act.getSellerId()+"]");
            	return;
            }
            
        	List<Long> list = this.getActUserList(act);
        	long time = System.currentTimeMillis();
            if (!CollectionUtils.isEmpty(list))
            {
                // 优惠券发送
                this.sendUMP(list, act);
            }
            logger.info(act.getSellerId()+"["+act.getActivityId()+"]===========  UMP发送===="+list.size()+"完成,耗时："+(System.currentTimeMillis()-time)/1000+"秒" );
        }
		
	}
	  protected void sendUMP(List<Long> sellerUserList, ActivityInfoDo info)
	    {

//	        ExecutorService pool = Executors.newFixedThreadPool(THREAD_NUM);
	        int size = sellerUserList.size();
	        if (size == 0)
	        {
	            return;
	        }

	        Long tagId = JsonActivityDetail.activityDetailJsonStrToBean(info.getActivityDetailgsontStr()).getTagId();
	        Long sellerId = info.getSellerId();

	        if (tagId == null)
	        {
	            return;
	        }

//	        logger.info(sellerId+"===========  UMP发送====size=" + size);

	        int begin = 0;
	        int step = 1000;// 每次只能发送 100张
	        int end = step;
	        if (size <= step)
	        {
	            end = size;
	        }
	        do
	        {
	            List<Long> list = sellerUserList.subList(begin, end);
	            // 通过会员分组id查询对应的会员。 根据sellerUserId找出买家的信息
	            List<BuyerDo> _userList = buyerDao.listByIds(info.getSellerId(),list );

	            ArrayList<Callable<Boolean>> callers = new ArrayList<Callable<Boolean>>();
	            int sn = 100;
	            for (int i = 0; i < _userList.size(); i += sn)
	            {
	                int startIndx = i;
	                int endIndex = startIndx + sn;
	                endIndex = endIndex > _userList.size() ? _userList.size() : endIndex;
	                List<BuyerDo> sublist = _userList.subList(startIndx, endIndex);
//	                callers.add(new UmpThread(tagId, sellerId, sublist));
	                Integer doSuccess = sellerSessionKeyService.tagUserSave(tagId, sellerId, sublist);
	            }
	            /*try
	            {
	                pool.invokeAll(callers);
	            } catch (InterruptedException e)
	            {
	                e.printStackTrace();
	            }*/

	            if (end >= size)
	            {
	                break;
	            }

	            begin = end;
	            end = end + step;
	            if (end > size)
	            {
	                end = size;
	            }

	        } while (true);

//	        pool.shutdown();
	    }

	    public List<Long> getActUserList(ActivityInfoDo info)
	    {
	        JsonActivityDetail activityDetail = null;
	        String fieldValues = null;
	        List<Long> list = null;

	        activityDetail = JsonActivityDetail.activityDetailJsonStrToBean(info.getActivityDetailgsontStr());
	        Long gropId = Long.valueOf(activityDetail.getGroupId());

	     	//遍历活动获取优惠标签信息，得出会员分组id。
	        UserDynamicGroupDo userDynamicGroup = new UserDynamicGroupDo();
	        userDynamicGroup.setSellerId(info.getSellerId());
	        userDynamicGroup.setGroupId(gropId);
	        userDynamicGroup = this.userDynamicGroupService.load(info.getSellerId(), gropId);
	        if (userDynamicGroup != null)
	        {
	            fieldValues = userDynamicGroup.getConditions();
	        }
//	        logger.info("================== " + fieldValues);

	        // BuyerSearchVO searchVO = new BuyerSearchVO();
	        // searchVO.addSellerId(info.getSellerId());
	        //两种方式，一种是手动导入昵称的，一种是筛选器筛选出来的，以sourceFrom作区分 2014—06-16 xingxing
	        if (buyerImportService != null && userDynamicGroup != null && userDynamicGroup.getSourceFrom() != null
	                && userDynamicGroup.getSourceFrom() == 1)
	        {
	            list = buyerImportService.listAllIdsByGroup(userDynamicGroup.getSellerId(), userDynamicGroup.getGroupId());
	        } else
	        {
	            if (fieldValues != null && StrUtils.isNotEmpty(fieldValues))
	            {
	                fieldValues = fieldValues.replace("'..", "\"..");
	                fieldValues = fieldValues.replace("gif'", "gif\"");

	                JsonParser parser = new JsonParser();
	                JsonArray ja = parser.parse(fieldValues).getAsJsonArray();
	                String[] fieldValue2 = new String[ja.size()];
	                List<String> quyerFieldJsonList = new ArrayList<String>();
	                for (int i = 0; i < ja.size(); i++)
	                {
	                    fieldValue2[i] = ja.get(i).getAsJsonObject().toString();
	                }
	                if (fieldValue2 != null && fieldValue2.length > 0)
	                {
	                    for (String str : fieldValue2)
	                    {
	                        if (StrUtils.isNotEmpty(str))
	                        {
	                            quyerFieldJsonList.add(str);
	                        }
	                    }
	                }

	                try
	                {
	                    list = getDolphinBuyerSearcher().searchAllBuyer(info.getSellerId(), quyerFieldJsonList);
	                } catch (IOException e)
	                {
	                	logger.info(StrUtils.showError(e));
	                    e.printStackTrace();
	                }
	            }
	        }
	        return list;
	    }

	    public class UmpThread implements Callable<Boolean> {

	        private Long      tagId;
	        private Long      sellerId;
	        private List<BuyerDo> list;

	        public UmpThread(Long tagId, Long sellerId, List<BuyerDo> list)
	        {
	            super();
	            this.tagId = tagId;
	            this.sellerId = sellerId;
	            this.list = list;
	        }

	        @Override
	        public Boolean call() throws Exception
	        {
	            List<String> buyerList = new ArrayList<String>();
	            // 发送
	            for (BuyerDo map : list)
	            {
	            	if (StrUtils.isNotEmpty(map.getNick())) {
	            		buyerList.add(map.getNick());
					}
	            	
	                
	            }
	            
	            //通过会员分组id查询对应的会员。
	        	//遍历会员调用sellerSessionKeyService.TmallPromotagTaguserSave进行打优惠标签
                Integer doSuccess = sellerSessionKeyService.TmallPromotagTaguserSave(tagId, sellerId, buyerList);
	            if (doSuccess>0) {
//            	   logger.info(sellerId + "==UMP发送====打标["+tagId+"]成功=>>>>>>>"+doSuccess+"个会员，总共有"
//                           + list.size());
               }
                   
	            return Boolean.TRUE;
	        }

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
	 
}
