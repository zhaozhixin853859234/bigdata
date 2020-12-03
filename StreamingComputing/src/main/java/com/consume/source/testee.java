package com.consume.source;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * <h3>bigdata</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-08-21 10:48
 */
public class testee {
    public static void main(String[] args) throws SQLException {
        Connection conn = JDBCUtil.jdbcConnection();
        Statement stmt = conn.createStatement();

        String sql = "select * from consume_record order by consume_time limit 100;";

        ResultSet rs = stmt.executeQuery(sql);


        while (rs.next()){
            int user_id = rs.getInt("user_id");
            int credit_id = rs.getInt("credit_id");
            String credit_type = rs.getString("credit_type");
            String consume_time = rs.getString("consume_time");
            String consume_city = rs.getString("consume_city");
            String consume_type = rs.getString("consume_type");
            int consume_money =  rs.getInt("consume_money");

            System.out.print(user_id+"  "+credit_id+"  "+credit_type+"  "+consume_time
                    +"  "+consume_city+"  "+consume_type+"  "+consume_money);
            System.out.println();

        }
        rs.close();
        stmt.close();
        conn.close();
    }
}
