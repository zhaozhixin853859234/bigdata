package com.Service;

 import java.sql.*;

/**
 * <h3>BigDataTest</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-05-31 20:27
 */
public class HiveSql {

    // 加载驱动
    public static void loadDriver(String driverName){
        try{
            Class.forName(driverName);
            System.out.println("加载驱动成功");
        } catch (ClassNotFoundException e) {
            System.out.println("加载驱动失败");
            e.printStackTrace();
        }
    }

    // 获取连接
     public static Connection getConnection(String hiveUrl,String user,String passWord) {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(hiveUrl, user, passWord);
            System.out.println("获取连接成功");
        } catch (SQLException e){
            e.printStackTrace();
            System.out.println("获取连接失败");
        }
        return conn;
     }

     // 获取执行环境statement
    public static Statement getStatement(Connection conn){
        Statement stmt = null;
        try{
            stmt =  conn.createStatement();
            System.out.println("获取执行环境成功");
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println("获取执行环境失败");
        }
        return stmt;
    }

    // 获取预编译执行环境PreparedStatement
    public static PreparedStatement getPreparedStatement(Connection conn,String sql){
        PreparedStatement pstmt = null;
        try{
            pstmt =  conn.prepareStatement(sql);
            System.out.println("获取执行环境并执行预编译sql成功");
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println("获取执行环境并执行预编译sql失败");
        }
        return pstmt;
    }

    // 执行sql查询，通过statement对象
    public static ResultSet getResultSet(Statement stmt,String sql){
        ResultSet res = null;
        try {
            res = stmt.executeQuery(sql);
            System.out.println("执行查询成功");
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println("执行查询失败");
        }
        return res;
    }

    //执行sql查询，通过PreparedStatement对象
    public  static ResultSet getResultSet(PreparedStatement pstmt){
        ResultSet res = null;
        try{
            res = pstmt.executeQuery();
            System.out.println("执行预编译查询成功");
        }catch (SQLException e){
            e.printStackTrace();
            System.out.println("执行预编译查询失败");
        }
        return res;
    }


}
