package com.pusidun.hadoop.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * HiveJDBC操作
 */
public class HiveJDBC {
    /**
     * 获取连接
     * @return Connection
     */
    public static Connection getConnection(){
        Connection conn = null;
        try {
            String driver = "org.apache.hive.jdbc.HiveDriver";
            String JDBCUrl = "jdbc:hive2://bigdata-cloudera-master.com:10000";
            String username = "";
            String password = "";
            Class.forName(driver);
            conn = DriverManager.getConnection(JDBCUrl,username,password);
        }catch (Exception e){
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 读取HBase数据
     */
    public static void readHBaseData(){
        try {
            Connection conn = getConnection();
            String sql = "select * from employee_hbase";
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()){
                System.out.println("rk:" + rs.getInt("rk")+"\tname:"+rs.getString("name")+"\tage:"+rs.getInt("age"));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        readHBaseData();
    }

}
