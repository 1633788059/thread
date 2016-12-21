package com.wangjubao.app.other.data.clean.service.impl;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.mail.Email;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wangjubao.app.other.data.clean.service.DataCleanService;
import com.wangjubao.app.other.data.clean.utils.ConnectionPool;
import com.wangjubao.dolphin.biz.model.SellerDo;
import com.wangjubao.dolphin.common.util.StrUtils;

/**
 * @author ckex created 2013-9-24 - 下午5:19:28 DataCleanServiceImpl.java
 * @explain -
 */
public class DataCleanServiceImpl implements DataCleanService {

    public transient final static Logger   logger           = LoggerFactory.getLogger("data-clean");

    private static Map<String, String>     DATA_SOURCE      = new HashMap<String, String>();

    private static Map<String, String>     DATA_SOURCE_TEST = new TreeMap<String, String>();

    //    private static String[]                tables      = new String[] { "t_email_sent", "t_refund_notify", "  t_trade_notify",
    //            "  t_activity_info", "  t_activity_result", " t_activity_result_detail", "  t_activity_send_content", "  t_buyer",
    //            "  t_buyer_day", "  t_item", "   t_order", "   t_trade", "  t_sms_log", " t_field_date_data", "  t_field_int_data",
    //            "   t_field_text_data", "   t_buyer_index_item", "   t_chat_buyer", "   t_seller_servicer ", "   t_exception_info ",
    //            "    t_customer_question", "    t_seller_servicer_session", "   t_customer_answer  ", "   t_user_form ", "t_coupon_log ",
    //            "t_user_service_event", "t_user_service_event_trail" };

    //    private static String[]                tables      = new String[] { "t_refund_notify",
    //            "  t_trade_notify", "  t_activity_info", "  t_activity_result",
    //            " t_activity_result_detail", "  t_activity_send_content", "    t_customer_question",
    //            "    t_seller_servicer_session", "   t_customer_answer  ", "   t_user_form ",
    //            "t_coupon_log ", "t_user_service_event", "t_user_service_event_trail",
    //            " t_field_date_data", "  t_field_int_data", "   t_field_text_data" };

    // "t_message_log","t_trans_process" -- 无sellerId
    private static String[]                ALL_TABLES       = new String[] { "t_activity_execute_detail", "t_activity_info",
            "t_activity_send_content", "t_black_list", "t_buyer_index_item", "t_chat_buyer", "t_content_email", "t_content_sms",
            "t_coupon_log", "t_customer_answer", "t_customer_question", "t_diagnosis_log", "t_email_log", "t_field_date_data",
            "t_field_int_data", "t_field_text_data", "t_his_import", "t_his_import_process", "t_history_trade_log", "t_item_cat",
            "t_member_list", "t_out_sms", "t_rfm_data", "t_seller_day", "t_seller_servicer", "t_seller_user", "t_seller_user_day",
            "t_sms_account", "t_sms_log", "t_sms_payment", "t_taobao_item", "t_taobao_order", "t_taobao_sellercat",
            "t_taobao_sync_log", "t_taobao_trade", "t_temp_middlecal", "t_trade_memo", "t_trade_rate", "t_trade_rule",
            "t_user_dynamic_group", "t_user_form", "t_user_group", "t_user_service_event", "t_user_service_event_trail",
            "t_ump_activitydetail", "t_ump_tool", "t_ump_activity", "t_user_event_history", "t_abnormal_trade_rule",
            "t_abnormal_trade", "t_report_sum", "t_report_order", "t_report_trade", "t_user_event_reported", "t_sms_log_history",
            "t_email_log_history", "t_email_account", "t_email_payment", "t_item", "t_trade_notify", "t_refund_notify",
            "t_coupon_detail", "t_activity_result", "t_activity_result_detail", "t_buyer", "t_trade", "t_order", "t_buyer_day",
            "t_seller_day_dolphin", "t_email_content", "t_email_sent", "t_email_template", "t_seller_abnormal_trade_rule",
            "t_seller_abnormal_trade_rule_group", "t_seller_traderate_rule", "t_seller_traderate_sign", "t_seller_traderate",
            "t_rate_sign", "t_user_dynamic_group_class", "t_focus_group_buyer", "t_focus_group_report", "t_focus_group_report_module",
            "t_trade_pack", "t_buyer_repay", "t_activity_reported", "t_email_sent_history", "t_activity_node", "t_activity_canvas",
            "t_activity_child_relevance", "t_activity_node_execute_list", "t_buyer_extend_key_values", "t_ucm_buyer_rel",
            "t_buyer_extend", "t_ucm_buyer_feature", "t_ucm_buyer", "t_super_trade_rel", "t_super_order_rel",
            "t_super_trade_rate_rel", "t_jifen_account", "t_jifen_account_log", "t_jifen_trade_rel", "t_jifen_item_rule",
            "t_jifen_record", "t_jifen_coupon", "t_jifen_coupon_rule", "t_email_feedback", "t_jifen_sign_in", "t_qn_role",
            "t_questionnaire", "t_naire_template", "t_template_element", "t_naire_backfill", "t_coupon_send_log", "t_naire_element",
            "t_buyer_import", "t_sms_replay"               };

    private static Map<String, Connection> CONN_MAP         = new HashMap<String, Connection>();

    static
    {
        DATA_SOURCE
                .put("000",
                        "jdbc:mysql://jconncavdwg2n.mysql.rds.aliyuncs.com:3306/saas?useUnicode=true&characterEncoding=utf8;jusrgy53bvv6;rds_wjb_2013");
        DATA_SOURCE
                .put("001",
                        "jdbc:mysql://conn0621i8q9.mysql.rds.aliyuncs.com:3306/saas001?useUnicode=true&characterEncoding=utf8;usr0621i8q9;rds_wjb_2013");
        DATA_SOURCE
                .put("002",
                        "jdbc:mysql://app-12638377.mysql.aliyun.com:3306/app_12638377?useUnicode=true&characterEncoding=utf8;app_12638377;rds_wjb_2013");
        DATA_SOURCE
                .put("003",
                        "jdbc:mysql://conn000130d8g3.mysql.rds.aliyuncs.com:3306/fangcaoji?useUnicode=true&characterEncoding=utf8;usr000130d8g3;rds_wjb_2013");
        DATA_SOURCE
                .put("004",
                        "jdbc:mysql://conn000130d8g3.mysql.rds.aliyuncs.com:3306/puxu?useUnicode=true&characterEncoding=utf8;usr000130d8g3;rds_wjb_2013");
        DATA_SOURCE
                .put("005",
                        "jdbc:mysql://conn0400p2u7.mysql.rds.aliyuncs.com:3306/boshideng?useUnicode=true&characterEncoding=utf8;usr0400p2u7;rds_wjb_2013");
        DATA_SOURCE
                .put("006",
                        "jdbc:mysql://conn0399t4n7.mysql.rds.aliyuncs.com:3306/yiji?useUnicode=true&characterEncoding=utf8;usr0399t4n7;rds_wjb_2013");
        DATA_SOURCE
                .put("007",
                        "jdbc:mysql://conn0399t4n7.mysql.rds.aliyuncs.com:3306/baiku?useUnicode=true&characterEncoding=utf8;usr0399t4n7;rds_wjb_2013");
        DATA_SOURCE
                .put("008",
                        "jdbc:mysql://jconncavdwg2n.mysql.rds.aliyuncs.com:3306/saas002?useUnicode=true&characterEncoding=utf8;jusrgy53bvv6;rds_wjb_2013");
        DATA_SOURCE
                .put("009",
                        "jdbc:mysql://conn0400p2u7.mysql.rds.aliyuncs.com:3306/saas003?useUnicode=true&characterEncoding=utf8;usr0400p2u7;rds_wjb_2013");
        DATA_SOURCE
                .put("010",
                        "jdbc:mysql://conn0399t4n7.mysql.rds.aliyuncs.com:3306/saas004?useUnicode=true&characterEncoding=utf8;usr0399t4n7;rds_wjb_2013");
        DATA_SOURCE
                .put("011",
                        "jdbc:mysql://conn0399t4n7.mysql.rds.aliyuncs.com:3306/saas005?useUnicode=true&characterEncoding=utf8;usr0399t4n7;rds_wjb_2013");
        DATA_SOURCE
                .put("012",
                        "jdbc:mysql://conn0771f7w9.mysql.rds.aliyuncs.com:3306/saas006?useUnicode=true&characterEncoding=utf8;usr0771f7w9;rds_wjb_2013");
        DATA_SOURCE
                .put("013",
                        "jdbc:mysql://conn0771f7w9.mysql.rds.aliyuncs.com:3306/saas007?useUnicode=true&characterEncoding=utf8;usr0771f7w9;rds_wjb_2013");
        DATA_SOURCE
                .put("014",
                        "jdbc:mysql://conn0771f7w9.mysql.rds.aliyuncs.com:3306/saas008?useUnicode=true&characterEncoding=utf8;usr0771f7w9;rds_wjb_2013");
        DATA_SOURCE
                .put("015",
                        "jdbc:mysql://conn0771f7w9.mysql.rds.aliyuncs.com:3306/saas009?useUnicode=true&characterEncoding=utf8;usr0771f7w9;rds_wjb_2013");
        DATA_SOURCE
                .put("016",
                        "jdbc:mysql://jconndgpxpfme.mysql.rds.aliyuncs.com:3306/saas010?useUnicode=true&characterEncoding=utf8;jusrprmgrsum;rds_wjb_2013");
        DATA_SOURCE
                .put("017",
                        "jdbc:mysql://jconndgpxpfme.mysql.rds.aliyuncs.com:3306/saas011?useUnicode=true&characterEncoding=utf8;jusrprmgrsum;rds_wjb_2013");
        DATA_SOURCE
                .put("018",
                        "jdbc:mysql://jconnicsmxhtg.mysql.rds.aliyuncs.com:3306/saas012?useUnicode=true&characterEncoding=utf8;jusrw9ezwc53;rds_wjb_2013");
        DATA_SOURCE
                .put("019",
                        "jdbc:mysql://jconnicsmxhtg.mysql.rds.aliyuncs.com:3306/saas013?useUnicode=true&characterEncoding=utf8;jusrw9ezwc53;rds_wjb_2013");
        DATA_SOURCE
                .put("020",
                        "jdbc:mysql://jconnig2xizi8.mysql.rds.aliyuncs.com:3306/saas020?useUnicode=true&characterEncoding=utf8;jusr3xnrqxwg;rds_wjb_2013");
        DATA_SOURCE
                .put("021",
                        "jdbc:mysql://jconnig2xizi8.mysql.rds.aliyuncs.com:3306/saas021?useUnicode=true&characterEncoding=utf8;jusr3xnrqxwg;rds_wjb_2013");
        DATA_SOURCE
                .put("022",
                        "jdbc:mysql://jconnig2xizi8.mysql.rds.aliyuncs.com:3306/saas022?useUnicode=true&characterEncoding=utf8;jusr3xnrqxwg;rds_wjb_2013");
        DATA_SOURCE
                .put("023",
                        "jdbc:mysql://jconnig2xizi8.mysql.rds.aliyuncs.com:3306/saas023?useUnicode=true&characterEncoding=utf8;jusr3xnrqxwg;rds_wjb_2013");
        DATA_SOURCE
                .put("024",
                        "jdbc:mysql://jconnhy3pt26x.mysql.rds.aliyuncs.com:3306/saas024?useUnicode=true&characterEncoding=utf8;jusrtv65nq9y;rds_wjb_2013");
        DATA_SOURCE
                .put("025",
                        "jdbc:mysql://jconnhy3pt26x.mysql.rds.aliyuncs.com:3306/saas025?useUnicode=true&characterEncoding=utf8;jusrtv65nq9y;rds_wjb_2013");
        DATA_SOURCE
                .put("026",
                        "jdbc:mysql://jconnhy3pt26x.mysql.rds.aliyuncs.com:3306/saas026?useUnicode=true&characterEncoding=utf8;jusrtv65nq9y;rds_wjb_2013");
        DATA_SOURCE
                .put("027",
                        "jdbc:mysql://jconnhy3pt26x.mysql.rds.aliyuncs.com:3306/saas027?useUnicode=true&characterEncoding=utf8;jusrtv65nq9y;rds_wjb_2013");
        DATA_SOURCE
                .put("028",
                        "jdbc:mysql://jconnareucbsf.mysql.rds.aliyuncs.com:3306/saas028?useUnicode=true&characterEncoding=utf8;jusrah7b2xg5;rds_wjb_2013");
        DATA_SOURCE
                .put("029",
                        "jdbc:mysql://jconnnw6yn2zh.mysql.rds.aliyuncs.com:3306/saas029?useUnicode=true&characterEncoding=utf8;jusrakjgwe6m;rds_wjb_2013");
        DATA_SOURCE
                .put("030",
                        "jdbc:mysql://jconnhtsgnk2a.mysql.rds.aliyuncs.com:3306/saas030?useUnicode=true&characterEncoding=utf8;jusrbt53at2n;rds_wjb_2013");
        DATA_SOURCE
                .put("031",
                        "jdbc:mysql://jconnyfpjdcff.mysql.rds.aliyuncs.com:3306/saas031?useUnicode=true&characterEncoding=utf8;jusrufiaditv;rds_wjb_2013");

        DATA_SOURCE
                .put("032",
                        "jdbc:mysql://jconnjg5jsj8f.mysql.rds.aliyuncs.com:3306/saas032?useUnicode=true&characterEncoding=utf8;jusrsatak6bu;rds_wjb_2013");

        DATA_SOURCE
                .put("033",
                        "jdbc:mysql://jconn8wd5hdes.mysql.rds.aliyuncs.com:3306/saas033?useUnicode=true&characterEncoding=utf8;jusrzs65jvqd;rds_wjb_2013");

        /************************************ TEST *****************************************/
        DATA_SOURCE_TEST.put("000", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("001", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("002", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("003", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("004", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("005", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("006", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("007", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("008", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("009", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("010", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("011", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("012", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("013", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("014", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("015", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("016", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("017", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("018", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("019", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("020", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("021", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("022", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("023", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("024", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("025", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("026", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("027", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("028", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("029", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("030", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("031", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("032", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
        DATA_SOURCE_TEST.put("033", "jdbc:mysql://223.5.23.49:3306/saas_dev?useUnicode=true&characterEncoding=utf8;root;riich123456");
    }

    @Override
    public void execute(SellerDo sellerDo)
    {
        long start = System.currentTimeMillis();
        logger.info(String.format("{%s【%s】} execute start ... ", sellerDo.getId(), sellerDo.getNick()));
        Set<Map.Entry<String, String>> set = DATA_SOURCE_TEST.entrySet(); // test
        for (Iterator<Map.Entry<String, String>> it = set.iterator(); it.hasNext();)
        {
            try
            {
                long s = System.currentTimeMillis();
                Map.Entry<String, String> entry = (Map.Entry<String, String>) it.next();
                String key = entry.getKey();
                if (!StringUtils.equalsIgnoreCase(key, sellerDo.getDatasourceKey()))
                { // 注意是非同一数据源

                    Connection connection = CONN_MAP.get(key);
                    if (connection == null)
                    {
                        ConnectionPool connectionPool = getColnnection(sellerDo, key);
                        connection = connectionPool.getConnection();
                        CONN_MAP.put(key, connection);
                    }
                    logger.info(entry.getKey() + " ###### " + entry.getValue());
                    startClean(connection, sellerDo, entry.getKey());
                    if (logger.isInfoEnabled())
                    {
                        logger.info(String.format("{%s【%s】}(%s) data clean finished. time:(%s)-ms", sellerDo.getId(),
                                sellerDo.getNick(), key, (System.currentTimeMillis() - s)));
                    }
                } else
                {
                    continue;
                }
            } catch (Exception e)
            {
                e.printStackTrace();
                logger.error(" xxx " + Arrays.toString(e.getStackTrace()));
            }
        }
        logger.info(String.format("{%s【%s】} clean all data finished times:(%s)-ms", sellerDo.getId(), sellerDo.getNick(),
                (System.currentTimeMillis() - start)));
    }

    private void startClean(Connection connection, SellerDo sellerDo, String dataSourceKey) throws SQLException
    {

        if (connection != null)
        {
            try
            {
                Statement statement = connection.createStatement();
                for (String table : ALL_TABLES)
                {
                    try
                    {
                        long start = System.currentTimeMillis();
                        String sql = "delete from " + table + " where sellerId = " + sellerDo.getId();
                        statement.execute(sql);
                        if (logger.isInfoEnabled())
                        {
                            logger.info(String.format("{%s【%s】-%s} data clean (%s - %s) finished.time:(%s)-ms ", sellerDo.getId(),
                                    sellerDo.getNick(), sellerDo.getDatasourceKey(), dataSourceKey, table,
                                    (System.currentTimeMillis() - start)));
                        }
                    } catch (Exception e)
                    {
                        e.printStackTrace();

                        DatabaseMetaData metaData = connection.getMetaData();

                        ResultSet rs = metaData.getTables(null, null, "%", null);
                        String base = null;
                        while (rs.next())
                        {
                            base = rs.getString(1);
                        }
                        logger.error(String.format("{%s【%s】} data clean (%s) {%s} error %s.", sellerDo.getId(), sellerDo.getNick(),
                                table, base, Arrays.toString(e.getStackTrace())));
                        continue;
                    }
                }
            } catch (SQLException e)
            {
                e.printStackTrace();
            }
        } else
        {
            logger.error(String.format("{%s【%s】} init ConnectionPool error . ", sellerDo.getId(), sellerDo.getNick()));
        }
    }

    private ConnectionPool getColnnection(SellerDo sellerDo, String dataSourceKey)
    {
        ConnectionPool connectionPool = null;
        String dataSource = DATA_SOURCE_TEST.get(dataSourceKey); //test
        if (StringUtils.isNotBlank(dataSource))
        {
            String[] connStr = dataSource.split(";");
            if (connStr.length == 3)
            {
                String url = connStr[0];
                String username = connStr[1];
                String password = connStr[2];
                connectionPool = ConnectionPool.getInstance(url, username, password);
            } else
            {
                logger.error(String.format("{%s【%s】} dataSources Invalid (%s). ", sellerDo.getId(), sellerDo.getNick(), dataSource));
            }
        } else
        {
            logger.error(String.format("{%s【%s】} dataSources is empty key (%s). ", sellerDo.getId(), sellerDo.getNick(), dataSourceKey));
        }
        return connectionPool;
    }

    @Override
    public void execute(List<SellerDo> list)
    {
        if (list == null || list.isEmpty())
        {
            logger.error(" sellers is empty . ");
            return;
        }
        int i = 0;
        int k = list.size();
        for (SellerDo sellerDo : list)
        {
            ++i;
            try
            {
                logger.info(String.format("{%s【%s】}(%s) {%s - %s} begin ...  ", sellerDo.getId(), sellerDo.getNick(),
                        sellerDo.getDatasourceKey(), k, i));
                if (StringUtils.isNotBlank(sellerDo.getDatasourceKey()) && sellerDo.getId() != null)
                {
                    this.execute(sellerDo);
                } else
                {
                    logger.error(String.format("{%s【%s】} empty . ", sellerDo.getId(), sellerDo.getNick()));
                }
            } catch (Exception e)
            {
                e.printStackTrace();
                logger.error(" xxx " + Arrays.toString(e.getStackTrace()));
            }
        }
    }

    @Override
    public void cleanDataBySourceKey(List<SellerDo> sellerList)
    {
        Map<String, List<SellerDo>> groupBySourceKey = new HashMap<String, List<SellerDo>>();
        for (Iterator<SellerDo> iterator = sellerList.iterator(); iterator.hasNext();)
        {
            SellerDo seller = iterator.next();
            List<SellerDo> list = groupBySourceKey.get(seller.getDatasourceKey());
            if (list == null)
            {
                list = new ArrayList<SellerDo>();
                groupBySourceKey.put(seller.getDatasourceKey(), list);
            }
            list.add(seller);
        }
        for (Iterator<Entry<String, List<SellerDo>>> iterator = groupBySourceKey.entrySet().iterator(); iterator.hasNext();)
        {
            Entry<String, List<SellerDo>> entry = iterator.next();
            String datasouceKey = entry.getKey();
            List<SellerDo> list = entry.getValue();
            startCleanData(datasouceKey, list);
        }
    }

    private void startCleanData(String datasouceKey, List<SellerDo> sellers)
    {
        StringBuilder sb = new StringBuilder();
        for (Iterator<SellerDo> iterator = sellers.iterator(); iterator.hasNext();)
        {
            SellerDo sellerDo = iterator.next();
            sb.append(",").append(sellerDo.getId());
        }
        String sellerIds = sb.toString().substring(1);
        Connection connection = CONN_MAP.get(datasouceKey);
        if (connection == null)
        {
            ConnectionPool connectionPool = getColnnection(datasouceKey);
            connection = connectionPool.getConnection();
            CONN_MAP.put(datasouceKey, connection);
        }
        logger.info(datasouceKey + " ###### " + sellerIds);

        try
        {
            Statement statement = connection.createStatement();
            for (String table : ALL_TABLES)
            {
                try
                {
                    long start = System.currentTimeMillis();
                    String sql = "delete from " + table + " where sellerId not in ( " + sellerIds + " )";
                    statement.execute(sql);
                    if (logger.isInfoEnabled())
                    {
                        logger.info(String.format("{%s} finished.time:(%s)-ms (%s) ", datasouceKey,
                                (System.currentTimeMillis() - start), sql));
                    }
                } catch (Exception e)
                {
                    e.printStackTrace();

                    DatabaseMetaData metaData = connection.getMetaData();

                    ResultSet rs = metaData.getTables(null, null, "%", null);
                    String base = null;
                    while (rs.next())
                    {
                        base = rs.getString(1);
                    }
                    logger.error(String.format(" data clean (%s) {%s} error %s.", table, base, Arrays.toString(e.getStackTrace())));
                    continue;
                }
            }
        } catch (SQLException e)
        {
            e.printStackTrace();
            logger.error(StrUtils.showError(e));
        }

    }

    private ConnectionPool getColnnection(String datasouceKey)
    {
        ConnectionPool connectionPool = null;
        String dataSource = DATA_SOURCE_TEST.get(datasouceKey); //test
        if (StringUtils.isNotBlank(dataSource))
        {
            String[] connStr = dataSource.split(";");
            if (connStr.length == 3)
            {
                String url = connStr[0];
                String username = connStr[1];
                String password = connStr[2];
                connectionPool = ConnectionPool.getInstance(url, username, password);
            }
        }
        return connectionPool;
    }

    public static void main(String[] args) throws InterruptedException
    {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        String name = runtime.getName(); // format: "pid@hostname"
        System.out.println(name);
        System.out.println(runtime.getClassPath());
    }
}
