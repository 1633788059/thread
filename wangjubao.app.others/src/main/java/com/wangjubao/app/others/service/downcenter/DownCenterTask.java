package com.wangjubao.app.others.service.downcenter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wangjubao.dolphin.biz.common.model.PageQuery;
import com.wangjubao.dolphin.biz.model.SysSyntaskDo;
import com.wangjubao.dolphin.biz.service.impl.SellerServiceImpl;

@Service
public class DownCenterTask implements Runnable{

	private final Logger logger = LoggerFactory.getLogger("downcenter");
	
	private ExecutorService   sellerThreadPool;
	private ExecutorService   marketingUploadExecutor;
	private ConcurrentHashMap<Long, Boolean>  runningJobs = new ConcurrentHashMap<Long, Boolean>();
/*
    private static final int THREAD_POOL_DEFAULT_CORE_POOL_SIZE = 1;
    private static final int THREAD_POOL_DEFAULT_MAXIMUM_POOL_SIZE = 1;
    private static final int THREAD_POOL_DEFAULT_KEEP_ALIVE_TIME = 5;
    private static final TimeUnit THREAD_POOL_DEFAULT_KEEP_ALIVE_TIME_UNIT = TimeUnit.SECONDS;
    private static final Integer THREAD_POOL_DEFAULT_WAIT_QUEUE_SIZE = Integer.MAX_VALUE;    
*/	    
	@Autowired
	private TaskCodeTypeService taskCodeTypeService;
	
	@Autowired
	private SellerServiceImpl sellerService;

	
	@Override
	public void run() {
		try{
			configureThreadPool();
			startTaskInThreadPool();
		}catch(Throwable t){
			logger.error("Fail to load task and execute", t);
		}
	}
	
	/**
	 * 初始化
	 * @return
	 */
	public void initDoing(){
		PageQuery pageQuery = new PageQuery(0, 200);
		SysSyntaskDo record = new SysSyntaskDo();
		record.setStatus("doing");
		while (true) {
			List<SysSyntaskDo> list = sellerService.listSysSyntaskList(record, pageQuery, 0);
			if (list==null || list.isEmpty()) {
				break;
			}
			for (SysSyntaskDo sysSyntaskDo : list) {
				DownCenterService centerService = this.taskCodeTypeService.getServiceByCodeType(sysSyntaskDo.getCode());
				if (centerService!=null) {
					centerService.init(sysSyntaskDo);
				}
			}
			pageQuery.increasePageNum();
		}
	}

	private void startTaskInThreadPool() {
		PageQuery pageQuery = new PageQuery(0, 200);
		SysSyntaskDo record = new SysSyntaskDo();
		record.setStatus("init");
		while (true) {
			logger.info("Start to retrieve download task======");
			List<SysSyntaskDo> list = sellerService.listSysSyntaskList(record, pageQuery, 0);
			if (list==null || list.isEmpty()) {
				break;
			}else{
				logger.info("{} tasks loaded from DB", list.size());
			}
			for (SysSyntaskDo sysSyntaskDo : list) {
				if(getToken(sysSyntaskDo.getId())){
					logger.info("task code is {}",sysSyntaskDo.getCode());
					DownCenterService centerService = this.taskCodeTypeService.getServiceByCodeType(sysSyntaskDo.getCode());
					if(centerService != null) {
						DownJobTask downJobTask = new DownJobTask(sysSyntaskDo, centerService, this);
						ExecutorService taskExecutor = getTaskExecutor(sysSyntaskDo.getCode());
						taskExecutor.execute(downJobTask);
					}else{
						releaseToken(sysSyntaskDo.getId());
						logger.error("Fail to obtain appreciate service to process task [{}]-{}", sysSyntaskDo.getSellerId(), sysSyntaskDo.getTitle());
					}
				}else{
					logger.info("{}, previous task is still execute, skip ", sysSyntaskDo.getSellerId());
				}
			}
			pageQuery.increasePageNum();
		}
		
	}
	
	private boolean getToken(Long taskId){
		if(runningJobs.putIfAbsent(taskId, Boolean.TRUE) == null)
			return true;
		return false;
	}
	
	public void releaseToken(Long taskId){
		runningJobs.remove(taskId);
	}

	private void configureThreadPool() {
//		sellerThreadPool= initThreadPool();
//		sellerThreadPool.setCorePoolSize(20);
//        sellerThreadPool.setMaximumPoolSize(100);
		if(sellerThreadPool == null){
			sellerThreadPool = Executors.newFixedThreadPool(2);
		}
		if(marketingUploadExecutor == null){
			marketingUploadExecutor = Executors.newFixedThreadPool(5);
		}
	}
	
	private ExecutorService getTaskExecutor(String code){
		if(SysSyntaskDo.TaskCodeType.MARKET_EMAIL_UPLOAD.toString().equals(code) 
				|| SysSyntaskDo.TaskCodeType.MARKET_MOBILE_UPLOAD.toString().equals(code))
			return marketingUploadExecutor;
		return sellerThreadPool;
	}
/*
	private ThreadPoolExecutor initThreadPool() {
    	if(sellerThreadPool == null)
    	{
	        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(THREAD_POOL_DEFAULT_CORE_POOL_SIZE,
	                THREAD_POOL_DEFAULT_MAXIMUM_POOL_SIZE, THREAD_POOL_DEFAULT_KEEP_ALIVE_TIME,
	                THREAD_POOL_DEFAULT_KEEP_ALIVE_TIME_UNIT, new LinkedBlockingQueue<Runnable>(THREAD_POOL_DEFAULT_WAIT_QUEUE_SIZE));
	        return threadPoolExecutor;
    	}
    	else
    	{
    		return sellerThreadPool;
    	}
    }
*/	
}
