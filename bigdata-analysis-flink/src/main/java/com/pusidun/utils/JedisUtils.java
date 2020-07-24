package com.pusidun.utils;

import redis.clients.jedis.Jedis;

/**
 * Jedis常用工具
 */
public class JedisUtils {

    //创建一个对象
    private static Jedis instance;

    //让构造函数为 private，这样该类就不会被实例化
    private JedisUtils(){}

    //唯一获取实例对象
    public static synchronized Jedis getInstance(){
        if( null == instance ){
            instance = new Jedis("localhost", 6379);
            instance.auth("123456");
        }
        return instance;
    }


}
