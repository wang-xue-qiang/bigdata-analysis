package com.pusidun.flink.streambatch.keywords;

import com.pusidun.utils.HBaseUtils;
import com.pusidun.utils.IkUtils;
import com.pusidun.utils.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import java.util.*;

/**
 * 根据用户搜索关键字（算法TF-UDF）：
 *   统计用户最爱浏览的商品
 *   get 'shop:user', '0000000002'
 */
public class KeyWordsTask {
    public static void main(String[] args) throws Exception {
        //1.设置执行环境
        ExecutionEnvironment env =  ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取文件
        DataSource<String> dataSource = env.readTextFile("./keywords.txt");
        MapOperator<KeyWordsEntity, KeyWordsEntity> mapSource  = dataSource.map(new MapFunction<String, KeyWordsEntity>() {
                    @Override
                    public KeyWordsEntity map(String s) throws Exception {
                        String[] dataArray = s.split("\t");
                        KeyWordsEntity entity = new KeyWordsEntity();
                        entity.setUid(dataArray[0]);
                        List<String> oriWordList = new ArrayList<>();
                        oriWordList.add(dataArray[1]);
                        entity.setOriWordList(oriWordList);
                        return entity;
                    }
                })
                .groupBy("uid")
                .reduce(new ReduceFunction<KeyWordsEntity>() {
                    @Override
                    public KeyWordsEntity reduce(KeyWordsEntity a, KeyWordsEntity b) throws Exception {
                        String uid = a.getUid();
                        List<String> originalWords = new ArrayList<String>();
                        originalWords.addAll(a.getOriWordList());
                        originalWords.addAll(b.getOriWordList());
                        KeyWordsEntity keyWordEntity = new KeyWordsEntity();
                        keyWordEntity.setUid(uid);
                        keyWordEntity.setOriWordList(originalWords);
                        return keyWordEntity;
                    }
                })
                .map(new MapFunction<KeyWordsEntity, KeyWordsEntity>() {
                    @Override
                    public KeyWordsEntity map(KeyWordsEntity entity) throws Exception {
                        //定义每个word出现的次数
                        Map<String, Long> tfMap = new HashMap<String, Long>();
                        //HBase存储关键字
                        Set<String> wordSet = new HashSet<String>();
                        //每个用户聚合后的
                        List<String> wordList = entity.getOriWordList();
                        for (String lineStr : wordList) {
                            //分词
                            List<String> listData = IkUtils.IkSplit(lineStr);
                            for (String word : listData) {
                                //本次统计之前出现的次数
                                long preCount = tfMap.get(word) == null ? 0l : tfMap.get(word);
                                long currCount = preCount + 1;
                                tfMap.put(word, currCount);
                                wordSet.add(word);
                            }
                        }
                        //计算文档总数
                        long sum = 0;
                        Collection<Long> longs = tfMap.values();
                        for (Long aLong : longs) {
                            sum += aLong;
                        }

                        //计算tf
                        Map<String, Double> tfMapFinal = new HashMap<>();
                        Set<Map.Entry<String, Long>> entrySet = tfMap.entrySet();
                        for (Map.Entry<String, Long> entry : entrySet) {
                            String word = entry.getKey();
                            Long wordCount = entry.getValue();
                            double tf = Double.valueOf(wordCount) / Double.valueOf(sum);
                            tfMapFinal.put(word, tf);
                        }

                        //更新HBase
                        for (String word : wordSet) {
                            String data = HBaseUtils.getData("shop:user", entity.getUid(), "info", "keyWordCount");
                            Long newCount = data == null ? 1l : Long.valueOf(data) + 1;
                            HBaseUtils.putData("shop:user", entity.getUid(), "info", "keyWordCount", newCount.toString());
                        }


                        //结果封装
                        KeyWordsEntity vo = new KeyWordsEntity();
                        vo.setUid(entity.getUid());
                        vo.setDataMap(tfMap);
                        vo.setTotalDocument(sum);
                        vo.setTfMap(tfMapFinal);
                        return vo;
                    }
                })
                .map(new MapFunction<KeyWordsEntity, KeyWordsEntity>() {

                    @Override
                    public KeyWordsEntity map(KeyWordsEntity entity) throws Exception {
                        //获取TfMap
                        Map<String, Double> tfMap = entity.getTfMap();
                        //获取总文档数量
                        Long totalDocument = entity.getTotalDocument();

                        //计算TF-IDF
                        Map<String,Double> tfidfMap = new HashMap<String,Double>();
                        Set<Map.Entry<String, Double>> entrySet = tfMap.entrySet();
                        for (Map.Entry<String, Double> map : entrySet) {
                            String word = map.getKey();
                            Double value = map.getValue();
                            String data = HBaseUtils.getData("shop:user", entity.getUid(), "info", "keyWordCount");
                            //此值位于分母，按照公式+1
                            long  wordCount = Long.valueOf(data)+1;
                            double tfidf = value * Math.log(totalDocument / wordCount);
                            tfidfMap.put(word,tfidf);
                        }

                        //将tf-idf进行排序
                        LinkedHashMap<String, Double> resultMap = MapUtils.sortMapByValue(tfidfMap);
                        Set<Map.Entry<String, Double>> enSet = resultMap.entrySet();
                        //最终关键字
                        List<String> finalWord = new ArrayList<String>();
                        int count = 1;
                        for (Map.Entry<String, Double> doubleEntry : enSet) {
                            finalWord.add(doubleEntry.getKey());
                            count = count +1;
                            //此处取用户排名前三的搜索
                            if(count > 3){break;}

                        }
                        //保存关键字到HBase
                        String keyWordStr = "";
                        for(String str : finalWord){
                            keyWordStr += str+",";
                        }
                        keyWordStr = keyWordStr.substring(0,keyWordStr.length()-1);
                        if(StringUtils.isNotEmpty(keyWordStr)){
                            HBaseUtils.putData("shop:user", entity.getUid(), "info", "keyWordLabel", keyWordStr);
                        }

                        //封装最后结果
                        KeyWordsEntity result = new KeyWordsEntity();
                        result.setUid(entity.getUid());
                        result.setFinalKeyWordsList(finalWord);
                        return result;
                    }
                });
        //触发action操作
        List<KeyWordsEntity> list = mapSource.collect();
        for (KeyWordsEntity entity : list) {
            System.out.println(entity);
        }
        //设置环境执行
        env.execute();

    }
}
