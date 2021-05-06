package com.pusidun.hbase.weibo;

import com.pusidun.hbase.utils.HBaseUtils;

import java.io.IOException;

/**
 * 测试微博功能
 */
public class WeiBoTest {

    public static void main(String[] args) throws IOException, InterruptedException {

        WeiBoDao.publishWeiBo("1001","用户1001：五月一日劳动节，放五天假，开心！O(∩_∩)O");
        WeiBoDao.addAttends("1002","1001","1003");
        WeiBoDao.init("1002");
        System.out.println("**********111**********");
        WeiBoDao.publishWeiBo("1003","用户1003：今天星期五"); Thread.sleep(10);
        WeiBoDao.publishWeiBo("1001","用户1001：哦，今天该放假了");Thread.sleep(10);
        WeiBoDao.publishWeiBo("1003","用户1003：五一准备干嘛");Thread.sleep(10);
        WeiBoDao.publishWeiBo("1001","用户1001：打游戏");Thread.sleep(10);
        WeiBoDao.init("1002");
        System.out.println("**********222**********");
        WeiBoDao.deleteAttends("1002","1001");
        WeiBoDao.init("1002");
        System.out.println("**********333**********");
        WeiBoDao.addAttends("1002","1001");
        WeiBoDao.init("1002");
        System.out.println("**********444**********");
        WeiBoDao.getWeiBo("1003"); 

    }


    //初始化表
    public static void weiboInit() {
        try {
            //HBaseUtils.createNameSpace(Constants.NAMESPACE);
            //HBaseUtils.createTable(Constants.CONTENT_TABLE,Constants.CONTENT_TABLE_VERSION,Constants.CONTENT_TABLE_CF);
            //HBaseUtils.createTable(Constants.RELATION_TABLE,Constants.RELATION_TABLE_VERSION,Constants.RELATION_TABLE_CF,Constants.RELATION_TABLE_CF2);
            HBaseUtils.createTable(Constants.INBOX_TABLE, Constants.INBOX_TABLE_VERSION, Constants.INBOX_TABLE_CF);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
