package com.wangjubao.app.others.job;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.wangjubao.core.dao.ICommonDao;
import com.wangjubao.core.util.SynContext;

/**
 * @author ckex
 */
@Deprecated
public class DelExceptionInfoJob implements Job {

    public transient final static Logger logger = Logger.getLogger("others");
    private static StringBuffer          sql    = new StringBuffer();
    private static ICommonDao            commonDao;
    private static String                token  = "null";

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        logger.info("exception del  start ... ... ");
        commonDao = (ICommonDao) SynContext.getObject("commonDao");
        queryException();
        logger.info("exception del  end ... ... ");
    }

    public void queryException() {
        logger.info("del exception ...");
        List<Long> list = new ArrayList<Long>();
        List<Map> mapIds = new ArrayList<Map>();
        sql.setLength(0);
        sql.append("SELECT sellerId as sellerId FROM t_exception_info GROUP BY sellerId;");
        mapIds = commonDao.queryListBySql(sql.toString());
        logger.info("length : " + mapIds.size());
        for (Map<String, Long> map : mapIds) {
            if (isNotBlank(map, "sellerId"))
                list.add(map.get("sellerId"));
            else
                list.add(null);
        }
        this.queryInfo(list);
    }

    private void queryInfo(List<Long> sellerIds) {
        for (int i = 0; i < sellerIds.size(); i++) {
            List<Long> list = new ArrayList<Long>();
            sql.setLength(0);
            Long sellerId = sellerIds.get(i);
            logger.info("sellerId : " + sellerId);
            if (sellerId != null && sellerId != 0) {
                sql.append("SELECT id as id FROM t_exception_info WHERE sellerId = "
                        + sellerIds.get(i) + " ORDER BY id DESC LIMIT 100;");
                List<Map> exceptionIds = commonDao.queryListBySql(sql.toString());
                for (Map<String, Long> map : exceptionIds) {
                    list.add(map.get("id"));
                }
                if (list.size() > 0)
                    this.delExceptionInfo(list, sellerIds.get(i));
                else
                    continue;
            } else {
                sql.append("SELECT id as id FROM t_exception_info WHERE sellerId IS NULL ORDER BY id DESC LIMIT 100;");
                List<Map> exceptionIds = commonDao.queryListBySql(sql.toString());
                for (Map<String, Long> map : exceptionIds) {
                    list.add(map.get("id"));
                }
                if (list.size() > 0)
                    this.delExceptionInfo(list);
                else
                    continue;
            }
        }
    }

    private void delExceptionInfo(List<Long> list, Long sellerId) {
        sql.setLength(0);
        sql.append("DELETE FROM t_exception_info WHERE  id IS NOT IN " + getIds(list)
                + "  AND sellerId = " + sellerId);
    }

    private void delExceptionInfo(List<Long> list) {
        sql.setLength(0);
        sql.append("DELETE FROM t_exception_info WHERE id IS NOT IN " + getIds(list)
                + "  AND sellerId IS NULL");
    }

    private String getIds(List<Long> list) {
        if (list.size() == 0)
            return null;
        StringBuffer sb = new StringBuffer();
        sb.append("(");
        for (Long str : list) {
            String s = str.toString();
            sb.append(s + ",");
        }
        String str = sb.toString();
        str = str.substring(0, str.length() - 1);
        str += ")";
        return str;
    }

    public static boolean isNotBlank(Map map, String key) {
        String value = map.get(key) + "";
        if (StringUtils.isBlank(value) || value.trim().length() < 0
                || value.equalsIgnoreCase(token))
            return false;
        else
            return true;
    }
}
