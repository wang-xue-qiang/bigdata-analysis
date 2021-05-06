package com.pusidun.hbase.weibo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * 常量类
 */
public class Constants {

    //HBase配置信息
    public static final Configuration CONFIGURATION = HBaseConfiguration.create();

    //命名空间
    public static final String NAMESPACE  = "weibo";

    //微博内容表
    public static final String CONTENT_TABLE = "weibo:content";
    public static final String CONTENT_TABLE_CF = "info";
    public static final int CONTENT_TABLE_VERSION = 1;

    //微博用户关系表
    public static final String RELATION_TABLE = "weibo:relation";
    public static final String RELATION_TABLE_CF = "attends";
    public static final String RELATION_TABLE_CF2 = "fans";
    public static final int RELATION_TABLE_VERSION = 1;

    //微博收件箱表
    public static final String INBOX_TABLE = "weibo:inbox";
    public static final String INBOX_TABLE_CF = "info";
    public static final int INBOX_TABLE_VERSION = 2;
}
