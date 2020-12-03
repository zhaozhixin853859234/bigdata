package com.Service;

import com.Entity.BucketInfo;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * <h3>BigDataTest</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-06-01 09:26
 */
public class HiveTest {
    private static final String DRIVERNAME = "org.apache.hive.jdbc.HiveDriver";
    private static final String HIVEURL = "jdbc:hive2://192.168.211.128:10000/default";
    private static final String USER = "";
    private static final String PASSWORD = "";
    public static void main(String[] args) throws SQLException {
        HiveSql.loadDriver(DRIVERNAME);
        Connection conn = HiveSql.getConnection(HIVEURL,USER,PASSWORD);
        Statement stmt = HiveSql.getStatement(conn);
        ResultSet res = HiveSql.getResultSet(stmt,"select * from test_bucketed limit 10");

        // 展示结果，封装成对象
        System.out.print("user_id  ");
        System.out.print("        access_time");
        System.out.println();
        List<BucketInfo> list = new LinkedList<BucketInfo>();
        while (res.next()){
            // 可以将读取的数据封装成对象
//            System.out.print(res.getString("user_id"));
//            System.out.print("    ");
//            System.out.print(res.getString("access_time"));
//            System.out.println();

            BucketInfo bi  = new BucketInfo();
            bi.setId(res.getString("user_id"));
            bi.setTime(res.getString("access_time"));
            list.add(bi);
        }

        for (BucketInfo bi :list) {
            System.out.println(bi.getId()+"   "+bi.getTime());
        }
    }
}


