package com.wangjubao.app.others.service.itemCatSync.impl;

import java.util.Date;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.taobao.api.domain.ItemCat;
import com.taobao.api.response.ItemcatsGetResponse;
import com.wangjubao.app.others.service.itemCatSync.ItemCatSyncService;
import com.wangjubao.dolphin.biz.dao.SellerDao;
import com.wangjubao.dolphin.biz.model.FeatureDo;
import com.wangjubao.dolphin.biz.model.ItemCatDo;
import com.wangjubao.dolphin.biz.model.ItemCatStandardDo;
import com.wangjubao.dolphin.biz.service.ItemCatStandardService;
import com.wangjubao.dolphin.biz.service.SellerSessionKeyService;
import com.wangjubao.dolphin.common.util.StrUtils;
import com.wangjubao.dolphin.common.util.date.DateUtils;
import com.wangjubao.framework.util.DateUtil;

@Service("itemCatSyncService")
public class ItemCatSyncServiceImpl implements ItemCatSyncService {

	 static final Logger                               logger       = Logger.getLogger("itemCatSyn");
	
	public static Gson       gson   = new Gson();

    public static JsonParser parser = new JsonParser();
	@Autowired
	private SellerSessionKeyService sellerSessionKeyService;
	

	private static final String FIELDS = "features,taosir_cat,cid,parent_cid,name,is_parent,status,sort_order";
    
    @Autowired
    private SellerDao sellerDao;
    
    @Autowired
    private ItemCatStandardService itemCatService;
    
	@PostConstruct
	@Override
	public void init() {
		// 初始化所有的类目，由于淘宝增量接口还未开发，所以初次就放入所有的类目信息，然后每天按卖家根据卖家cid同步更新。
		
		
	}
	
	@Override
	public void syncItemCat() {
		logger.info(" ####################################### 拉类目列表信息启动..." + DateUtils.formatDate(DateUtils.now()));

        new Thread() {
            public void run() {
                while (true) {
                    //凌晨4点到6点执行
                    String tstart = "40000";
                    Integer endt = Integer.parseInt(tstart) + 160000;
                    String timenow = DateUtil.getTimeStr().replace(":", "");
                    boolean timeIsOk = false;
                    if (Integer.parseInt(timenow) >= Integer.parseInt(tstart)
                            && Integer.parseInt(timenow) <= endt) {
                        timeIsOk = true;
                    }

                    if (!timeIsOk) {
                        try {
                            sleep(1000 * 60 * 30);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        continue;
                    }

                    execute(0l);

                    try {
                        sleep(1000 * 60 * 60 );
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();
    
		
	}
	
	@Override
	public void execute(Long parentCid) {

            long start = System.currentTimeMillis();
            logger.info(String.format("{%s}begin synItemCat, time : 【%s】\t",
            		parentCid, DateUtils.formatDate(DateUtils.now())));
            
            ItemcatsGetResponse res = this.sellerSessionKeyService.getItemCats(new Date(), "", FIELDS, parentCid , 3l);
            
            if (res!=null) {
				synItemCat(res);
			}
            
            logger.info(String.format(
                    "{%s} synItemCat finished（%s）-ms | time : 【%s】\t \n",
                    parentCid,(System.currentTimeMillis() - start),
                     DateUtils.formatDate(DateUtils.now())));
    
		
	}

	private void synItemCat(ItemcatsGetResponse res) {
		if (res.getItemCats()!=null && res.getItemCats().size()>0) {
			for (ItemCat itemCat : res.getItemCats()) {
				try {
					ItemCatStandardDo itemCatDo = new ItemCatStandardDo();
					itemCatDo.setCid(itemCat.getCid().toString());
					if (itemCat.getFeatures() != null
							&& !itemCat.getFeatures().isEmpty()) {
						try {
							itemCatDo.setFeatures(gson.toJson(
									itemCat.getFeatures(),
									new TypeToken<List<FeatureDo>>() {
									}.getType()));
						} catch (Exception e) {
							logger.info("报错了++++++++++"+StrUtils.showError(e));
						}
					}
					itemCatDo.setIsParent(itemCat.getIsParent());
					itemCatDo.setName(itemCat.getName());
					itemCatDo.setParentCid(itemCat.getParentCid());
					itemCatDo.setSortOrder(Integer.valueOf(itemCat
							.getSortOrder().toString()));
					itemCatDo.setStatus(itemCat.getStatus());
//					itemCatDo.setTaosirCat(itemCat.get);
					itemCatDo.setVersion(1);
					itemCatDo = this.itemCatService
							.createOrUpdateCatDo(itemCatDo);
					if (itemCat.getIsParent()) {
						execute(itemCat.getCid());
					}
				} catch (Exception e) {
					logger.info("报错了++++++++++"+StrUtils.showError(e));
				}
			}
		}
	}
	

}
