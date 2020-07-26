package com.pusidun.elasticsearch;

import com.pusidun.utils.EsClientUtils;
import com.pusidun.utils.InfoUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 创建文档
 */
public class CreateIndex {
    public static void main(String[] args) throws InterruptedException {
        EsClientUtils esClient = EsClientUtils.getInstance();
        while (true){
            Map userMap = new HashMap();
            String uid =  InfoUtils.get32UUID();
            String name = InfoUtils.getChineseName();
            int age = InfoUtils.getNum(18, 45);
            String phone = InfoUtils.getTel();
            String email = InfoUtils.getEmail(10, 12);
            String address = InfoUtils.getRoad();
            userMap.put("uid",uid);
            userMap.put("name",name);
            userMap.put("age",age);
            userMap.put("phone",phone);
            userMap.put("email",email);
            userMap.put("address",address);
            esClient.addIndexMap("shop-user","shop-user",userMap);
            Thread.sleep(200);
        }
    }
}
