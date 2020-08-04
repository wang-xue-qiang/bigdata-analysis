package com.pusidun.elasticsearch;

import com.pusidun.utils.InfoUtils;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 测试ES增删查改和聚合
 */
public class TestES {

    /**
     * 测试
     * @param args 参数
     */
    public static void main(String[] args) {
        String index = "shop-user";
        String type = "shop-user";
        //创建索引
        //createIndex(index, type);
        //查询单个索引
        //getOne(index,type,"bwz1uHMBngYiDPYOIE-T");
        //多条件And查询
        //getOneByNameAndAge(index,type);
        //多条件or查询
        getOneByNameOrAge(index,type);
    }

    /**
     * 获取客户端
     * @return TransportClient
     */
    public static TransportClient getClient() {
        Settings settings = Settings
                .builder()
                .put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", false)
                .build();
        try {
            TransportClient client = new PreBuiltTransportClient(settings)
                    .addTransportAddresses(new TransportAddress(InetAddress.getByName("192.168.12.137"), 9300));
            return client;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 创建索引
     * @param index   索引：相当于mysql库
     * @param type    类型：相当于mysql表
     */
    public static void createIndex(String index, String type) {
        TransportClient client = getClient();
        System.out.println(client);
        while (true) {
            client.prepareIndex(index, type).setSource(getDataMap()).get();
            try {
                Thread.sleep(200);
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    /**
     * 查询单个索引 相当于mysql语句 select * from tableName  where id=1 ;
     * @param index 索引：相当于mysql库
     * @param type  类型：相当于mysql表
     * @param id 索引唯一标识
     */
    public static void getOne(String index, String type, String id){
        TransportClient client = getClient();
        GetResponse response = client.prepareGet(index, type, id)
                .setOperationThreaded(false)
                .get();
        Map<String, Object> fields = response.getSource();
        System.out.println("根据Id查询索引值："+fields);
        client.close();
    }

    /**
     * 根据多条件组合与查询   相当于mysql语句 select * from tableName where name='雍佳' and age=30;
     * @param index 索引：相当于mysql库
     * @param type 类型：相当于mysql表
     */
    public static void getOneByNameAndAge(String index, String type){
        TransportClient client = getClient();
        //封装查询条件must相当于and
        QueryBuilder qb = QueryBuilders.boolQuery()
                //.must(QueryBuilders.termQuery("name","焦影琰"))
                .must(QueryBuilders.termQuery("age",21)) ;
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(qb)
                .setFrom(0)
                .setSize(10)
                .get();
        SearchHits searchHits =  response.getHits();
        if(searchHits.getTotalHits() == 0){
            System.err.println("很遗憾，没有命中结果！");
        }else {
            System.err.println("==============多条件AND查询结果开始====================");
            for (SearchHit hit : searchHits.getHits()) {
                System.out.println(hit.getSourceAsString());
            }
        }
        client.close();
    }

    /**
     * 根据多条件组合与查询   相当于mysql语句 select * from tableName where name='游筠' or age=21;
     * @param index 索引：相当于mysql库
     * @param type 类型：相当于mysql表
     */
    public static void getOneByNameOrAge(String index, String type){
        TransportClient client = getClient();
        //两个条件或查询
        QueryBuilder s = QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name","游筠"))
                .should(QueryBuilders.termQuery("age",45));
        //根据age降序
        SortBuilder sortBuilder= SortBuilders.fieldSort("age");
        sortBuilder.order(SortOrder.DESC);
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(s)
                .addSort(sortBuilder)
                .setFrom(0)
                .setSize(10)
                .get();
        SearchHits searchHits =  response.getHits();
        if(searchHits.getTotalHits() == 0){
            System.err.println("很遗憾，没有命中结果！");
        }else {
            System.err.println("==============多条件Or查询结果开始====================");
            for (SearchHit hit : searchHits.getHits()) {
                System.out.println(hit.getSourceAsString());
            }
        }
        client.close();
    }

    /**
     * 制造 map数据
     * @return map
     */
    public static Map getDataMap() {
        Map<String, Object> map = new HashMap<>();
        String uid = InfoUtils.get32UUID();
        String name = InfoUtils.getChineseName();
        int age = InfoUtils.getNum(18, 45);
        String phone = InfoUtils.getTel();
        String email = InfoUtils.getEmail(10, 12);
        String address = InfoUtils.getRoad();
        map.put("uid", uid);
        map.put("name", name);
        map.put("age", age);
        map.put("phone", phone);
        map.put("email", email);
        map.put("address", address);
        map.put("create_time", new Date());
        return map;
    }


}
