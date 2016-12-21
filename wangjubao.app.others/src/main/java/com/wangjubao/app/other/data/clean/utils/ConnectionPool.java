/**
 * 
 */
package com.wangjubao.app.other.data.clean.utils;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Vector;

/**
 * @author ckex created 2013-9-24 - 下午3:41:27 ConnectionPool.java
 * @explain -
 */
public class ConnectionPool {

    private Vector<Connection>    pool;

    private String                url;

    private String                username;

    private String                password;

    private String                driverClassName;

    /**
     * 连接池的大小，也就是连接池中有多少个数据库连接。
     */

    private int                   poolSize = 2;

    private static ConnectionPool instance = null;

    /**
     * 私有的构造方法，禁止外部创建本类的对象，要想获得本类的对象，通过<code>getIstance</code>方法。 使用了设计模式中的单子模式。
     */

    //    private ConnectionPool() {
    //
    //        init();
    //
    //    }

    public ConnectionPool(String url, String username, String password) {
        super();
        this.url = url;
        this.username = username;
        this.password = password;
        this.driverClassName = "com.mysql.jdbc.Driver";
        init();
    }

    /**
     * 连接池初始化方法，读取属性文件的内容 建立连接池中的初始连接
     */

    private void init() {

        pool = new Vector<Connection>(poolSize);

        //        readConfig();

        addConnection();

    }

    /**
     * 返回连接到连接池中
     */

    public synchronized void release(Connection conn) {

        pool.add(conn);

    }

    /**
     * 关闭连接池中的所有数据库连接
     */

    public synchronized void closePool() {

        for (int i = 0; i < pool.size(); i++) {

            try {
                Connection conn = pool.get(i);

                if (conn != null) {

                    conn.close();

                }

            } catch (SQLException e) {

                e.printStackTrace();

            }

            pool.remove(i);

        }

    }

    /**
     * 返回当前连接池的一个对象
     */

    public static ConnectionPool getInstance(String url, String username, String password) {

        //        if (instance == null) {

        instance = new ConnectionPool(url, username, password);

        //        }

        return instance;

    }

    /**
     * 返回连接池中的一个数据库连接
     */

    public synchronized Connection getConnection() {

        if (pool.size() > 0) {

            Connection conn = pool.get(0);

            pool.remove(conn);

            return conn;

        } else {

            return null;

        }

    }

    /**
     * 在连接池中创建初始设置的的数据库连接
     */

    private void addConnection() {

        Connection conn = null;

        for (int i = 0; i < poolSize; i++) {

            try {

                Class.forName(driverClassName);

                conn = java.sql.DriverManager.getConnection(url, username, password);

                pool.add(conn);

            } catch (ClassNotFoundException e) {

                e.printStackTrace();

            } catch (SQLException e) {

                e.printStackTrace();

            }

        }

    }

    /**
     * 读取设置连接池的属性文件
     */

    private void readConfig() {

        try {

            String path = System.getProperty("user.dir") + "\\dbpool.properties";

            FileInputStream is = new FileInputStream(path);

            Properties props = new Properties();

            props.load(is);

            this.driverClassName = props.getProperty("driverClassName");

            this.username = props.getProperty("username");

            this.password = props.getProperty("password");

            this.url = props.getProperty("url");

            this.poolSize = Integer.parseInt(props.getProperty("poolSize"));

        } catch (Exception e) {

            e.printStackTrace();

            System.err.println("读取属性文件出错. ");

        }

    }

}
