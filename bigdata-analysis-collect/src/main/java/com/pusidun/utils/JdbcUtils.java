package com.pusidun.utils;
import java.sql.*;

/**
 * jdbc数据库
 */
public class JdbcUtils {

    private static final String USER = "root";
    private static final String PWD = "123456";
    private static final String URL = "jdbc:mysql://localhost:3306/shop";
    private static final String DRIVER = "com.mysql.jdbc.Driver";

    /**
     * 注册驱动(可以省略)
     */
    static {
        try {
            Class.forName(DRIVER); }
        catch (ClassNotFoundException e) { e.printStackTrace(); }
    }

    /**
     * 得到数据库的连接
     */
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PWD);
    }

    /**
     * 关闭所有打开的资源
     */
    public static void close(Connection conn, Statement stmt){
        if(stmt != null) {
            try { stmt.close(); } catch (SQLException e) { e.printStackTrace(); }
        }
        if(conn != null) {
            try { conn.close(); }catch (SQLException e) { e.printStackTrace(); }
        }

    }

    /**
     * 关闭所有打开的资源 重载
     */
    public static void close(Connection conn, Statement stmt, ResultSet rs) {
        if(rs != null) {
            try { rs.close(); } catch (SQLException e) { e.printStackTrace(); }
        }
        close(conn, stmt);
    }

}
