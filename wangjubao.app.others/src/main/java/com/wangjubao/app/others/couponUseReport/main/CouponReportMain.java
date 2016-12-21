package com.wangjubao.app.others.couponUseReport.main;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.wangjubao.app.others.service.couponReport.impl.CouponReportConsumeTask;
import com.wangjubao.app.others.service.couponReport.impl.CouponReportCreateTask;
import com.wangjubao.app.others.service.couponReport.impl.CouponSellerMap;
import com.wangjubao.app.others.service.couponReport.impl.CouponTaskDownFile;
import com.wangjubao.app.others.service.couponReport.impl.CouponTaskParseData;
import com.wangjubao.core.util.SynContext;

/**
 * @author luorixin  
 * @date 2014-5-23 下午4:45:13
 * @version V1.0 
 * @ClassName: CouponReportMain
 * @Description: 优惠券发送效果报告
 */
public class CouponReportMain
{
	static final Logger logger = Logger.getLogger("couponconsumetask");
    
    private static CouponReportMain main ;
    
    private static CouponSellerMap couponSellerMap;
    
    private static CouponReportCreateTask couponReportCreateTask;
    
    private static CouponReportConsumeTask couponReportConsumeTask;
    
    public void init() throws Exception {
        ApplicationContext context = new ClassPathXmlApplicationContext(
                new String[] { "classpath*:/spring/*.xml", "classpath*:/spring/index/*.xml",
                        "classpath*:META-INF/spring/buyersearch/*.xml",
                        "classpath*:META-INF/spring/*.xml" });
        SynContext.setContext(context);
    }
    
    public static void main(String[] args) throws Exception
    {
        logger.info("=== 启动  " + System.getProperty("user.dir"));
        main = new CouponReportMain();
        main.init();
        
        //查出符合条件的所有卖家
        logger.info("==== 查询卖家 启动==============");
        couponSellerMap = new CouponSellerMap();
        couponSellerMap.doWork();
        
        
        
      //根据卖家查询活动生成对应的任务
        logger.info("==== 生成task 启动==============");
        while (true) {
        	logger.info(".");
            if (CouponSellerMap.hasInitOver) {
                couponReportCreateTask = new CouponReportCreateTask();
                couponReportCreateTask.doWork();
                break;
            }
            Thread.sleep(1000);
        }
        
        //oldInit();
        newInit();
       
    }

	/*public static void oldInit() throws InterruptedException {
        
        //根据任务id插入优惠券的效果报告
        logger.info("==== 消费task 启动==============");
        while (true) {
            if (CouponSellerMap.hasInitOver) {
                couponReportConsumeTask = new CouponReportConsumeTask();
                couponReportConsumeTask.doWork();
                break;
            }
            Thread.sleep(1000);
        }
	}*/
	
    private static CouponTaskDownFile couponTaskDownFile;
    
    private static CouponTaskParseData couponTaskParseData;
    public static void newInit() throws Exception{
    	while (true) {
    		logger.info(".");
            if (CouponSellerMap.hasInitOver) {
            	couponTaskDownFile=new CouponTaskDownFile();
            	couponTaskParseData=new CouponTaskParseData();
            	logger.info("==== 文件下载 task 启动==============");
            	couponTaskDownFile.doWork();
            	
            	logger.info("==== 数据解析 task 启动==============");
            	couponTaskParseData.doWork();
            	
                break;
            }
            Thread.sleep(1000);
        }
    }
}
