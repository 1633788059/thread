/**
 * 
 */
package com.wangjubao.app.other.data.clean.main;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wangjubao.app.other.data.clean.service.DataCleanService;
import com.wangjubao.app.other.data.clean.service.impl.DataCleanServiceImpl;
import com.wangjubao.app.other.data.clean.utils.ConnectionPool;
import com.wangjubao.dolphin.biz.model.SellerDo;

/**
 * @author ckex created 2013-9-24 - 下午3:48:31 Main.java
 * @explain -
 */
public class Main {

    public transient final static Logger      logger    = LoggerFactory.getLogger("data-clean");

    private final static Map<String, Integer> COUNT_MAP = new HashMap<String, Integer>();

    //    echo '主库'  mysql -hrds01.aliyun.com -uusr000130d8g3  -prds_wjb_2013 saas_base 

    public static void main(String[] args) throws SQLException
    {
        //        countData();
        testCleanData();
    }

    private static void countData() throws SQLException
    {
        logger.info("  ####################################### start ... ");
        long start = System.currentTimeMillis();
        ConnectionPool connectionPool = ConnectionPool.getInstance(
                "jdbc:mysql://223.5.20.113:3306/smsemay241?useUnicode=true&amp;characterset=utf-8", "root", "xplazy_123456");

        Connection connection = connectionPool.getConnection();

        Statement statement = connection.createStatement();

        int count = 0;

        StringBuffer sb = new StringBuffer(
                "SELECT id,content FROM send where state = 1111 and intime = '2013-11-11 00:00:00' order by id LIMIT  ");
        int pageSize = 5000;
        int pageNo = 0;
        //        sql = "SELECT id,content FROM send1 order by id LIMIT 0,10";
        //        String sql = sb.append(pageNo).append(",").append(pageSize).toString();
        //        System.out.println(sql);
        String sql = null;
        while (true)
        {
            start = System.currentTimeMillis();
            StringBuffer string = new StringBuffer(sb);
            sql = string.append(pageNo * pageSize).append(",").append(pageSize).toString();
            //            System.out.println(" sql " + sql);
            ResultSet resultSet = null;
            resultSet = statement.executeQuery(sql);
            int c = 0;
            while (resultSet.next())
            {
                //                System.out.println(resultSet.getString(1) + " " + resultSet.getString(2));
                String content = resultSet.getString(2);
                String sign = content.substring(content.lastIndexOf("【") + 1, content.lastIndexOf("】"));
                //                System.err.println("sign " + sign);
                COUNT_MAP.put(sign, (COUNT_MAP.get(sign) == null ? 0 : COUNT_MAP.get(sign)) + 1);
                c++;
                count++;
            }
            System.out.println("下一页码：" + pageNo + " 完成总数:  " + count + " 耗时： " + (System.currentTimeMillis() - start));
            if (c < pageSize)
            {
                break;
            } else
            {
                pageNo++;
            }
        }

        //        sql = "select count(*) from send1 where state = 1111";
        //        sql = sb.append(pageNo).append(",").append(pageSize).toString();
        //        ResultSet resultSet = statement.executeQuery(sql);

        //        while (resultSet.next()) {
        //            System.out.println(count + "   " + resultSet.getString(1));
        //        }

        for (Iterator<Entry<String, Integer>> it = COUNT_MAP.entrySet().iterator(); it.hasNext();)
        {
            Entry<String, Integer> entry = it.next();
            System.out.println("签名：" + entry.getKey() + " 数量：" + entry.getValue());
        }

    }

    private static void testCleanData() throws SQLException
    {

        long start = System.currentTimeMillis();
        ConnectionPool connectionPool = ConnectionPool.getInstance(
                "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8", "root", "riich123456");

        Connection connection = connectionPool.getConnection();

        Statement statement = connection.createStatement();

        ResultSet resultSet = statement.executeQuery("select * from t_seller");

        List<SellerDo> sellerList = new ArrayList<SellerDo>();

        while (resultSet.next())
        {
            SellerDo sellerDo = new SellerDo();
            sellerDo.setId(resultSet.getLong("id"));
            sellerDo.setNick(resultSet.getString("nick"));
            sellerDo.setDatasourceKey(resultSet.getString("datasource_key"));
            //            if (sellerDo.getId().longValue() == 120055122l) {
            sellerList.add(sellerDo);
            //            }
        }
        connection.close();
        connectionPool.closePool();
        DataCleanService service = new DataCleanServiceImpl();
        //        service.execute(sellerList);
        service.cleanDataBySourceKey(sellerList);
        logger.info(" data clean all finished. times : " + (System.currentTimeMillis() - start) + " -ms");
    }

    private static void testData() throws SQLException
    {

        logger.info("  #######################################3 start ... ");
        long start = System.currentTimeMillis();
        ConnectionPool connectionPool = ConnectionPool.getInstance(
                "jdbc:mysql://rds01.aliyun.com:3306/saas_test00?useUnicode=true&amp;characterset=utf-8", "usr000130d8g3",
                "rds_wjb_2013");

        //        ConnectionPool connectionPool = ConnectionPool.getInstance(
        //                "jdbc:mysql://localhost:3306/saas4?useUnicode=true&amp;characterset=utf-8", "root", "1234");

        Connection connection = connectionPool.getConnection();

        Statement statement = connection.createStatement();

        Integer c = 0;
        String[] tables = new String[] { "sms_wait_batch", "sms_wait_single", "sms_wait_synamic" };
        while (true)
        {
            ++c;
            for (int i = 0; i < tables.length; i++)
            {
                String tableName = tables[i];
                StringBuffer sb = new StringBuffer("insert into " + tableName
                        + " (`MessageID`,`Content`,`Demobile`,`LongID`,`Creatime`,`SendTime`) VALUES ");
                for (int j = 0; j < 1000; j++)
                {

                    String messageId = tableName.substring(tableName.length() - 1, tableName.length()) + j + "" + c.toString() + ""
                            + System.currentTimeMillis();

                    sb.append("('" + messageId + "','test国都single','15601662650','1', NOW(),NOW()),");
                }
                String sql = sb.toString().substring(0, (sb.toString().length() - 1));
                try
                {
                    statement.execute(sql);
                } catch (Exception e)
                {
                    e.printStackTrace();
                }
                logger.info(" insert " + c * 1000);
            }

            if (c > 200)
            {
                break;
            }
        }

    }

    public static void clanData(String[] args) throws SQLException
    {

        long start = System.currentTimeMillis();
        ConnectionPool connectionPool = ConnectionPool.getInstance(
                "jdbc:mysql://conn000130d8g3.mysql.rds.aliyuncs.com:3306/saas_base?useUnicode=true&characterEncoding=utf8",
                "usr000130d8g3", "rds_wjb_2013");

        Connection connection = connectionPool.getConnection();

        Statement statement = connection.createStatement();

        ResultSet resultSet = statement.executeQuery("select * from t_seller");

        List<SellerDo> sellerList = new ArrayList<SellerDo>();

        while (resultSet.next())
        {
            SellerDo sellerDo = new SellerDo();
            sellerDo.setId(resultSet.getLong("id"));
            sellerDo.setNick(resultSet.getString("nick"));
            sellerDo.setDatasourceKey(resultSet.getString("datasource_key"));
            //            if (sellerDo.getId().longValue() == 120055122l) {
            sellerList.add(sellerDo);
            //            }
        }
        connection.close();
        connectionPool.closePool();
        DataCleanService service = new DataCleanServiceImpl();
        //        service.execute(sellerList);
        service.cleanDataBySourceKey(sellerList);
        logger.info(" data clean all finished. times : " + (System.currentTimeMillis() - start) + " -ms");
    }

    private static void test() throws SQLException
    {

        //        ConnectionPool connectionPool = ConnectionPool.getInstance(
        //                "jdbc:mysql://localhost:3306/saas4?useUnicode=true&characterEncoding=utf8", "root", "1234");
        //        Connection connection = connectionPool.getConnection();

        ConnectionPool connectionPool = ConnectionPool.getInstance(
                "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8", "root", "riich123456");
        Connection connection = connectionPool.getConnection();

        Statement statement = connection.createStatement();
        //        DatabaseMetaData metaData = connection.getMetaData();
        //        ResultSet rs = metaData.getTables(null, null, "%", null);

        //        while (rs.next()) {
        //            tables.add(rs.getString(3));
        //            ResultSet set = statement.executeQuery("desc " + rs.getString(3));
        //            System.out.println(" ---- " + rs.getShort(3));
        //        }

        //        for (String table : tables) {
        //            String sql = " Select count(*) as count from " + table + " WHERE sellerId = "
        //                    + sellerId;
        //            ResultSet resultSet = statement.executeQuery(sql);
        //            while (resultSet.next()) {
        //                System.out.println(table + " : " + resultSet.getString("count"));
        //            }
        //        }

        ResultSet resultSet = statement.executeQuery("select * from t_seller");
        while (resultSet.next())
        { // 判断是否还有下一个数据  
            System.out.println(resultSet.getString("nick") + " " + resultSet.getLong("id") + "  "
                    + resultSet.getString("datasource_key"));
        }

    }
}
