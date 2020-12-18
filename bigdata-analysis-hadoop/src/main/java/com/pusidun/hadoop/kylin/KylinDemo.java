package com.pusidun.hadoop.kylin;

import java.sql.*;

public class KylinDemo {
    public static void main(String[] args) throws Exception{
        //Kylin_JDBC 驱动
        String KYLIN_DRIVER = "org.apache.kylin.jdbc.Driver";
        //Kylin_URL
        String KYLIN_URL = "jdbc:kylin://192.168.12.139:7070/Bigdata";
        //Kylin的用户名
        String KYLIN_USER = "ADMIN";
        //Kylin的密码
        String KYLIN_PASSWD = "KYLIN";
        //添加驱动信息
        Class.forName(KYLIN_DRIVER);
        //获取连接
        Connection connection = DriverManager.getConnection(KYLIN_URL, KYLIN_USER, KYLIN_PASSWD);
        //预编译SQL
        PreparedStatement ps = connection.prepareStatement("select dname,sum(sal) from emp e join dept d on e.deptno = d.deptno group by dname;");
        //执行查询
        ResultSet resultSet = ps.executeQuery();
        //遍历打印
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1)+"\t"+resultSet.getInt(2));
        }
    }
}
