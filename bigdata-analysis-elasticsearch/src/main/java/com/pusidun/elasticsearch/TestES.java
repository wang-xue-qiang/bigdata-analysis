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
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.net.InetAddress;
import java.util.*;


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
/*        getById(index,type,"bwz1uHMBngYiDPYOIE-T");
        //多条件And查询
        getByNameAndAge(index,type);
        //多条件or查询
        getByNameOrAge(index,type);
        //模糊查询
        getByNameLike(index,type);
        //年龄范围
        getByAgeRange(index,type);
        //指定name集合
        getByNameIn(index,type);*/
        System.out.println("================聚合查询=================");
        //年龄大于等于30并且小于等于40的总人数
        getCountByAge(index,type);
        //年龄大于等于30并且小于等于40的最大的值
        getMaxAge(index,type);
        //年龄大于等于30并且小于等于40的平均年龄
        getAvgAge(index,type);
        //年龄大于等于30并且小于等于40的年龄总和
        getSumAge(index,type);
        //年龄大于等于30并且小于等于40的各项指标
        getAgeStats(index,type);
        //
        getGroupByAge(index,type);
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
                    .addTransportAddresses(new TransportAddress(InetAddress.getByName("192.168.12.130"), 9300));
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
     * @param index 索引
     * @param type  类型
     * @param id 索引唯一标识
     */
    public static void getById(String index, String type, String id){
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
     * @param index 索引
     * @param type 类型
     */
    public static void getByNameAndAge(String index, String type){
        TransportClient client = getClient();
        //封装查询条件must相当于and
        QueryBuilder qb = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchPhraseQuery("name","宫子克"))
                .must(QueryBuilders.termQuery("age",40)) ;
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(qb)
                .setFrom(0)
                .setSize(10)
                .get();
        SearchHits searchHits =  response.getHits();
        if(searchHits.getTotalHits() == 0){
            System.err.println("很遗憾，多条件AND查询没有命中结果！");
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
     * @param index 索引
     * @param type 类型
     * should(QueryBuilders.matchQuery("xm","好的"))//分词后匹配
     * should(QueryBuilders.matchPhraseQuery("addr","钱江路"))//匹配完整词
     * should(QueryBuilders.termQuery("status",0))//完全匹配
     * should(QueryBuilders.termsQuery("keyword",string[]))//多关键字匹配
     */
    public static void getByNameOrAge(String index, String type){
        TransportClient client = getClient();
        //两个条件或查询
        QueryBuilder s = QueryBuilders.boolQuery()
                .should(QueryBuilders.matchPhraseQuery("name","游筠"))
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
     * 模糊查询相当于mysql的like
     * @param index 索引
     * @param type 类型
     */
    public static void getByNameLike(String index, String type){
        TransportClient client = getClient();
        QueryBuilder query = QueryBuilders.wildcardQuery("name", "赵*");
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(query)
                .setFrom(0)
                .setSize(10)
                .get();
        SearchHits searchHits =  response.getHits();
        if(searchHits.getTotalHits() == 0){
            System.err.println("很遗憾，没有命中结果！");
        }else {
            System.err.println("==============模糊查询结果开始====================");
            for (SearchHit hit : searchHits.getHits()) {
                System.out.println(hit.getSourceAsString());
            }
        }
        client.close();
    }


    /**
     * 年龄范围查询
     * @param index 索引
     * @param type 类型
     */
    public static void getByAgeRange(String index, String type){
        TransportClient client = getClient();
        //QueryBuilder query = QueryBuilders.rangeQuery("age").gte(30 ); //大于30
        QueryBuilder query = QueryBuilders.rangeQuery("age").from(30,true).to(31,true);
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(query)
                .setFrom(0)
                .setSize(10)
                .get();
        SearchHits searchHits =  response.getHits();
        if(searchHits.getTotalHits() == 0){
            System.err.println("很遗憾，没有命中结果！");
        }else {
            System.err.println("==============年龄范围查询结果开始====================");
            for (SearchHit hit : searchHits.getHits()) {
                System.out.println(hit.getSourceAsString());
            }
        }
        client.close();
    }


    /**
     * 指定姓名集合相当于 in
     * @param index 索引
     * @param type 类型
     */
    public static void getByNameIn(String index, String type){
        TransportClient client = getClient();
        List<String> list = new ArrayList<>();
        list.add("夔娥");
        list.add("连行江");
        list.add("酆晶仪");
        QueryBuilder query = QueryBuilders.matchPhraseQuery("name",list);
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(query)
                .setFrom(0)
                .setSize(10)
                .get();
        SearchHits searchHits =  response.getHits();
        if(searchHits.getTotalHits() == 0){
            System.err.println("很遗憾，指定姓名集合没有命中结果！");
        }else {
            System.err.println("==============指定姓名集合结果开始====================");
            for (SearchHit hit : searchHits.getHits()) {
                System.out.println(hit.getSourceAsString());
            }
        }
        client.close();
    }

    /**
     * 年龄大于等于30并且小于等于40的总人数
     * @param index 索引
     * @param type 类型
     */
    public static void getCountByAge(String index, String type){
        TransportClient client = getClient();
        AggregationBuilder termsBuilder = AggregationBuilders.count("ageCount").field("age");
        QueryBuilder query = QueryBuilders.rangeQuery("age").from(30,true).to(40,true);
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(query)
                .addAggregation(termsBuilder)
                .get();
        SearchHits searchHits =  response.getHits();
        if(searchHits.getTotalHits() == 0){
            System.err.println("很遗憾，指定年龄范围的总数没有命中结果！");
        }else {
            ValueCount valueCount= response.getAggregations().get("ageCount");
            long value = valueCount.getValue();
            System.out.println("年龄大于等于30并且小于等于40的年龄人群中的人数是："+value);
        }
        client.close();
    }


    /**
     * 查询年龄大于等于30并且小于等于40的总和
     * @param index 索引
     * @param type 类型
     */
    public static void getSumAge(String index, String type){
        TransportClient client = getClient();
        AggregationBuilder termsBuilder = AggregationBuilders.sum("sumAge").field("age");
        QueryBuilder query = QueryBuilders.rangeQuery("age").from(30,true).to(40,true);
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(query)
                .addAggregation(termsBuilder)
                .get();
        SearchHits searchHits =  response.getHits();
        if(searchHits.getTotalHits() == 0){
            System.err.println("很遗憾，年龄总和没有命中结果！");
        }else {
            Sum valueCount= response.getAggregations().get("sumAge");
            double value = valueCount.getValue();
            System.out.println("年龄大于等于30并且小于等于40的年龄人群中的年龄总和是："+value);
        }
        client.close();
    }

    /**
     * 查询年龄大于等于30并且小于等于40的平均年龄
     * @param index 索引
     * @param type 类型
     */
    public static void getAvgAge(String index, String type){
        TransportClient client = getClient();
        AggregationBuilder termsBuilder = AggregationBuilders.avg("avgAge").field("age");
        QueryBuilder query = QueryBuilders.rangeQuery("age").from(30,true).to(40,true);
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(query)
                .addAggregation(termsBuilder)
                .get();
        SearchHits searchHits =  response.getHits();
        if(searchHits.getTotalHits() == 0){
            System.err.println("很遗憾，查询查询平均年龄没有命中结果！");
        }else {
            Avg valueCount= response.getAggregations().get("avgAge");
            double value = valueCount.getValue();
            System.out.println("年龄大于等于30并且小于等于40的年龄人群中平均年龄是："+value);
        }
        client.close();
    }

    /**
     * 查询最大年龄
     * @param index 索引
     * @param type 类型
     */
    public static void getMaxAge(String index, String type){
        TransportClient client = getClient();
        AggregationBuilder termsBuilder = AggregationBuilders.max("maxAge").field("age");
        QueryBuilder query = QueryBuilders.rangeQuery("age").from(30,true).to(40,true);
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(query)
                .addAggregation(termsBuilder)
                .get();
        SearchHits searchHits =  response.getHits();
        if(searchHits.getTotalHits() == 0){
            System.err.println("很遗憾，查询最大年龄没有命中结果！");
        }else {
            Max valueCount= response.getAggregations().get("maxAge");
            double value = valueCount.getValue();
            System.out.println("年龄大于等于30并且小于等于40的年龄人群中的最大年龄是："+value);
        }
        client.close();
    }

    /**
     * 查询年龄指标
     * @param index 索引
     * @param type 类型
     */
    public static void getAgeStats(String index, String type){
        TransportClient client = getClient();
        AggregationBuilder termsBuilder = AggregationBuilders.stats("ageStats").field("age");
        QueryBuilder query = QueryBuilders.rangeQuery("age").from(30,true).to(40,true);
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(query)
                .addAggregation(termsBuilder)
                .get();
        SearchHits searchHits =  response.getHits();
        if(searchHits.getTotalHits() == 0){
            System.err.println("很遗憾，年龄指标没有命中结果！");
        }else {
            Stats valueCount = response.getAggregations().get("ageStats");
            System.out.println("max："+valueCount.getMaxAsString());
            System.out.println("avg："+valueCount.getAvgAsString());
            System.out.println("sum："+valueCount.getSumAsString());
            System.out.println("min："+valueCount.getMinAsString());
            System.out.println("count："+valueCount.getCount());
        }
        client.close();
    }

    /**
     * 年龄分组聚合
     * @param index 索引
     * @param type 类型
     */
    public static void getGroupByAge(String index, String type){
        TransportClient client = getClient();

        AggregationBuilder  termsBuilder = AggregationBuilders.terms("by_age").field("age");
        AggregationBuilder  sumBuilder   = AggregationBuilders.sum("ageSum").field("age");
        AggregationBuilder  avgBuilder   = AggregationBuilders.avg("ageAvg").field("age");
        AggregationBuilder  countBuilder = AggregationBuilders.count("ageCount").field("age");
        termsBuilder.subAggregation(sumBuilder).subAggregation(avgBuilder).subAggregation(countBuilder);

        QueryBuilder query = QueryBuilders.rangeQuery("age").from(30,true).to(40,true);
        //根据age降序
        SortBuilder sortBuilder = SortBuilders.fieldSort("age");
        sortBuilder.order(SortOrder.DESC);
        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setQuery(query)
                .addAggregation(termsBuilder)
                .addSort(sortBuilder)
                .get();
        Aggregations terms = response.getAggregations();
        for (Aggregation a : terms) {
            LongTerms teamSum = (LongTerms)a;
            for (LongTerms.Bucket bucket : teamSum.getBuckets()) {
                System.out.println(bucket.getKeyAsString()+"   "+bucket.getDocCount()+"    "+
                        ((Sum)bucket.getAggregations().asMap().get("ageSum")).getValue()+"    "+
                        ((Avg)bucket.getAggregations().asMap().get("ageAvg")).getValue()+"    "+
                        ((ValueCount)bucket.getAggregations().asMap().get("ageCount")).getValue());
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
