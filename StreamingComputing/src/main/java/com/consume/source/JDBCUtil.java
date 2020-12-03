package com.consume.source;


import java.sql.*;

/**
 * <h3>bigdata</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-08-21 10:35
 */
public class JDBCUtil {
    static final String USER = "root";
    static final String PASS = "root";

    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:3306/consume?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

    public static Connection jdbcConnection() {
        Connection conn = null;
        Statement stmt = null;
        try {
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);

            // 打开链接
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            return conn;

//            // 执行查询
//            System.out.println(" 实例化Statement对象...");
//            stmt = conn.createStatement();
//            String sql;
//            sql = "SELECT id, name, url FROM websites";
//            ResultSet rs = stmt.executeQuery(sql);
        }catch (Exception e){
            e.printStackTrace();
        }

        return conn;
    }
}
