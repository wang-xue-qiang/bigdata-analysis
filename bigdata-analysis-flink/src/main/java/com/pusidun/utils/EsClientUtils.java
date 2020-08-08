package com.pusidun.utils;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.Date;
import java.util.Map;

/**
 * es写入单列
 */
public class EsClientUtils {
    //es客户端
    private static  TransportClient transportClient ;
    //静态代码块
    {
        Settings settings = Settings
                .builder()
                .put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", false)
                .build();
        try{
            transportClient = new PreBuiltTransportClient(settings)
                    .addTransportAddresses(new TransportAddress(InetAddress.getByName("192.168.12.130"),9300));
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /**
     * 单例模式确保全局中只有一份该实例
     */
    private static class EsUtilsHolder{
        private static EsClientUtils instance = new EsClientUtils();
    }


    /**
     * 延迟加载，避免启动加载
     * @return
     */
    public static EsClientUtils getInstance(){
        return EsUtilsHolder.instance;
    }



    /**
     * map类型
     * @param index
     * @param type
     * @param map
     */
    public  void addIndexMap(String index, String type, Map<String,Object> map) {
        TransportClient client = null;
        try {
            if(null == map.get("create_time")){
                map.put("create_time",new Date());
            }
            transportClient.prepareIndex(index, type).setSource(map).get();
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            if(null != client) {
                client.close();
            }
        }
    }

}
