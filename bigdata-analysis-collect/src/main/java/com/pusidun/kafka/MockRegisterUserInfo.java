package com.pusidun.kafka;

import com.pusidun.utils.InfoUtils;
import com.pusidun.utils.JdbcUtils;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.Statement;

/**
 * 该数据用于Flink测试用户信息导入HBase
 */
public class MockRegisterUserInfo {

    public static void main(String[] args) throws Exception {
        //insertDatatoMysql();
        genFileData();
    }

    /**
     * 生成用户信息到mysql
     */
    private static void insertDatatoMysql(){
        Connection conn = null;
        Statement st = null;
        try{
            conn = JdbcUtils.getConnection();
            st = conn.createStatement();
            for (int i = 1; i < 10000; i++) {
                String uid = String.format("%010d", i);
                String name = InfoUtils.getChineseName();
                int age = InfoUtils.getNum(18, 45);
                String phone = InfoUtils.getTel();
                String email = InfoUtils.getEmail(10, 12);
                String address = InfoUtils.getRoad();
                st.executeUpdate("insert into shop_user (uid,name,age,phone,email,address) values ('"+uid+"','"+name+"','"+age+"','"+phone+"','"+email+"','"+address+"')");
            }

        }catch(Exception e){
            System.out.println(e);
        }finally {
            JdbcUtils.close(conn,st);
        }
    }


    /**
     * 生成数据到文件
     */
    private static void genFileData(){
        try {
            FileOutputStream fos = new FileOutputStream("./userInfo.txt",true);
            for (int i = 1; i <= 10000 ; i++) {
                String msg = String.format("%010d", i) + "\t" + InfoUtils.getChineseName() + "\t" +
                        InfoUtils.getNum(18, 45) + "\t" + InfoUtils.getTel() + "\t" +
                        InfoUtils.getEmail(10, 12) + "\t" + InfoUtils.getRoad()+"\n";
                fos.write(msg.getBytes());
            }
            fos.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
