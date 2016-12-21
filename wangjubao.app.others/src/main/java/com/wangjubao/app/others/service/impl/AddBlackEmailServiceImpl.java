/**
 * 
 */
package com.wangjubao.app.others.service.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.csvreader.CsvReader;
import com.wangjubao.app.others.service.AddBlackEmailService;
import com.wangjubao.dolphin.biz.cache.BuyerCacheDao;
import com.wangjubao.dolphin.biz.dao.EmailBlackDao;
import com.wangjubao.dolphin.biz.dao.sequence.SequenceSupport;
import com.wangjubao.dolphin.biz.model.EmailBlackDo;
import com.wangjubao.dolphin.common.util.date.DateUtils;

/**
 * @author ckex created 2013-11-25 - 下午4:19:21 AddBlackEmailServiceImpl.java
 * @explain -
 */
@Service("addBlackEmailService")
public class AddBlackEmailServiceImpl implements AddBlackEmailService {

    private static Logger       logger       = LoggerFactory.getLogger("AnomalyLogistics");

    private static final String EMAIL_REGEX  = "^(\\w)+(\\.\\w+)*@(\\w)+(\\.\\w+)*((\\.(com|cn|net){1,3}+)+)$";

    /**
     * 文件地址
     */
    //    private static String       DEFAULT_PATH = "C:\\Users\\Administrator\\Desktop\\需求文件\\dmd_blacklist.csv";
    private static String       DEFAULT_PATH = "/home/deploy/blackemails/";

    /**
     * 文件字符集
     */
    private static String       CHARSET      = "UTF-8";

    @Autowired
    private EmailBlackDao       emailBlackDao;

    @Autowired
    private SequenceSupport     sequenceSupport;

    @Autowired
    private BuyerCacheDao       buyerCacheDao;

    @Override
    public Boolean importBlackEmail()
    {
        File file = new File(DEFAULT_PATH);
        if (file.exists() && file.isDirectory() && file.canRead())
        {
            File[] files = file.listFiles();
            logger.info(" all files : " + Arrays.toString(files));
            for (File f : files)
            {
                if (!f.getName().endsWith(".csv"))
                {
                    logger.info(" skip : " + f.getAbsolutePath());
                    continue;
                }
                logger.info("  ---------- start . " + f.getAbsolutePath());
                CsvReader reader = null;
                try
                {
                    reader = new CsvReader(f.getAbsolutePath(), ',', Charset.forName(CHARSET));
                    reader.readHeaders();
                    int i = 0;
                    List<EmailBlackDo> blacklist = new ArrayList<EmailBlackDo>();
                    long start = System.currentTimeMillis();
                    while (reader.readRecord())
                    {
                        try
                        {
                            String[] csvline = reader.getValues();
                            String email = csvline[0];
                            boolean isLine0 = StringUtils.isNotBlank(email) && Pattern.matches(EMAIL_REGEX, email);
                            if (!isLine0)
                            {
                                email = csvline[3];
                                boolean isLine3 = StringUtils.isNotBlank(email) && Pattern.matches(EMAIL_REGEX, email);
                                if (!isLine3)
                                {
                                    continue;
                                }
                            }
                            //                        if (StringUtils.isNotBlank(email) && Pattern.matches(EMAIL_REGEX, email))
                            //                        {
                            EmailBlackDo emailBlackDo = new EmailBlackDo();
                            emailBlackDo.setId(sequenceSupport.nextTradeSeq());
                            emailBlackDo.setEmail(email);
                            emailBlackDo.setSource("webpower");
                            emailBlackDo.setStatus(0); //新添加的
                            blacklist.add(emailBlackDo);
                            if (i % 512 == 0)
                            {
                                if (blacklist != null && !blacklist.isEmpty())
                                {

                                    if (emailBlackDao.batchCreate(blacklist))
                                    {
                                        logger.info(" ------------------ 批处理完成:" + blacklist.size() + " 条,times:"
                                                + (System.currentTimeMillis() - start));
                                        blacklist = new ArrayList<EmailBlackDo>();
                                        start = System.currentTimeMillis();
                                    } else
                                    {
                                        logger.error("----------> 写入黑名单失败 ");
                                    }
                                }
                            }
                            buyerCacheDao.setBlackEmail(emailBlackDo.getEmail(), DateUtils.now());
                            ++i;
                            //                        }
                        } catch (Exception e)
                        {
                            e.printStackTrace();
                        }
                    }

                    if (blacklist != null && !blacklist.isEmpty())
                    {
                        if (emailBlackDao.batchCreate(blacklist))
                        {
                            blacklist = new ArrayList<EmailBlackDo>();
                        } else
                        {
                            logger.error("----------> 写入黑名单失败 ");
                        }
                    }

                } catch (FileNotFoundException e)
                {
                    e.printStackTrace();
                } catch (IOException e)
                {
                    e.printStackTrace();
                }
                reader = null;
            }
            System.gc();
            return Boolean.TRUE;
        } else
        {
            System.out.println(" ------------------ 路径不对或权限不够.");
        }
        return Boolean.FALSE;
    }

    @Override
    public Boolean importBlackMobiles()
    {
        List<String> mobiles = readTextFile();
        int step = 1000;
        int size = mobiles.size();
        for (int i = 0; i < mobiles.size(); i += step)
        {
            int startIndx = i;
            int endIndex = startIndx + step;
            endIndex = endIndex > size ? size : endIndex;
            List<String> list = mobiles.subList(startIndx, endIndex);
            batchImportMobile(list);
        }
        return true;
    }

    private void batchImportMobile(List<String> list)
    {
        List<EmailBlackDo> blacklist = new ArrayList<EmailBlackDo>();
        for (Iterator<String> iterator = list.iterator(); iterator.hasNext();)
        {
            String mobile = iterator.next();
            EmailBlackDo emailBlackDo = new EmailBlackDo();
            emailBlackDo.setId(sequenceSupport.nextTradeSeq());
            emailBlackDo.setEmail(mobile);
            emailBlackDo.setSource("webpower_mobile");
            emailBlackDo.setStatus(0); //新添加的
            blacklist.add(emailBlackDo);
            buyerCacheDao.setBlackEmail(emailBlackDo.getEmail(), DateUtils.now());
        }
        boolean isSuccess = emailBlackDao.batchCreate(blacklist);
        logger.info(" 导入手机黑名单" + (isSuccess ? "成功" : "失败") + ",共:" + blacklist.size() + " 条.");
    }

    public static void main(String[] args)
    {
        List<String> mobiles = readTextFile();
        System.out.println(">>>>>>>>>>:" + mobiles.size());
        for (String string : mobiles)
        {
            System.out.println(string);
        }
    }

    public static List<String> readTextFile()
    {
        String filePath = "./docs/blackmobilelist.csv"; //短信黑名单
        filePath = File.separator + "home" + File.separator + "deploy" + File.separator + "blackemails" + File.separator
                + "blackmobilelist.csv";
        //        StringBuffer result = new StringBuffer();
        List<String> results = new ArrayList<String>();
        try
        {
            String encoding = "UTF-8";
            File file = new File(filePath);
            if (file.isFile() && file.exists())
            { //判断文件是否存在
                InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);//考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                while ((lineTxt = bufferedReader.readLine()) != null)
                {
                    //                    result.append(lineTxt);
                    if (StringUtils.isNotBlank(lineTxt))
                    {
                        results.add(lineTxt);
                    }
                }
                read.close();
            } else
            {
                System.out.println("找不到指定的文件" + filePath + " isFile:" + file.isFile() + "  exists:" + file.exists());
            }
        } catch (Exception e)
        {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }
        return results;
    }
}
