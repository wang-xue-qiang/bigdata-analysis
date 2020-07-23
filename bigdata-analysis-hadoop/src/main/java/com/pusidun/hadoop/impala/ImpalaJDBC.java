package com.pusidun.hadoop.impala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Impala是Cloudera公司主导开发的新型查询系统，它提供SQL语义，能查询存储在Hadoop的HDFS和HBase中的PB级大数据。
 * 已有的Hive系统虽然也提供了SQL语义，但由于Hive底层执行使用的是MapReduce引擎，仍然是一个批处理过程，难以满足查询的交互性。
 * 相比之下，Impala的最大特点也是最大卖点就是它的快速。
 *
 * Hive加载HBase
 * CREATE EXTERNAL TABLE employee_hbase(
 * rk string,
 * name string,
 * age int)
 * ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
 * STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
 * WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, info:name, info:age")
 * TBLPROPERTIES("hbase.table.name" = "employee");
 *
 * 在impala同步数据元信息
 * INVALIDATE METADATA
 */
public class ImpalaJDBC {

    /**
     * 测试
     * @param args 参数
     */
    public static void main(String[] args) throws Exception {
        readHBaseData();
        System.out.println("======================================================");
        readImpalaData();
    }

    /**
     * 获取连接
     * @return Connection
     */
    public static Connection getConnection(){
        Connection conn = null;
        try {
            String driver = "org.apache.hive.jdbc.HiveDriver";
            String JDBCUrl = "jdbc:hive2://bigdata-cloudera-master.com:21050/;auth=noSasl";
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
            String sql = "select rk,name,age from employee_hbase";
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()){
                System.out.println("rk:" + rs.getInt("rk")+"\tname:"+rs.getString("name")+"\tage:"+rs.getInt("age"));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 读取Impala数据
     */
    public static void readImpalaData(){
        try{
            Connection conn = getConnection();
            String sql = "select id,name,age from games.employee order by id";
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()){
                System.out.println("id:" + rs.getInt("id")+"\tname:"+rs.getString("name")+"\tage:"+rs.getInt("age"));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
