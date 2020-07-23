package com.pusidun.flink.streambatch.keywords;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 用户搜索关键字实体类
 */
public class KeyWordsEntity implements Serializable {

    private String uid; //用户唯一标识
    private List<String> oriWordList; //用户搜索关键字
    private List<String> finalKeyWordsList; //最终关键词
    private Map<String, Long> dataMap; //记录关键字出现次数
    private Map<String, Double> tfMap; //记录关键字tf
    private Long totalDocument;       //文档总数


    public KeyWordsEntity() {
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public List<String> getOriWordList() {
        return oriWordList;
    }

    public void setOriWordList(List<String> oriWordList) {
        this.oriWordList = oriWordList;
    }

    public List<String> getFinalKeyWordsList() {
        return finalKeyWordsList;
    }

    public void setFinalKeyWordsList(List<String> finalKeyWordsList) {
        this.finalKeyWordsList = finalKeyWordsList;
    }

    public Map<String, Long> getDataMap() {
        return dataMap;
    }

    public void setDataMap(Map<String, Long> dataMap) {
        this.dataMap = dataMap;
    }

    public Map<String, Double> getTfMap() {
        return tfMap;
    }

    public void setTfMap(Map<String, Double> tfMap) {
        this.tfMap = tfMap;
    }

    public Long getTotalDocument() {
        return totalDocument;
    }

    public void setTotalDocument(Long totalDocument) {
        this.totalDocument = totalDocument;
    }

    @Override
    public String toString() {
        return uid +"\t" +String.join("-",finalKeyWordsList);
    }
}
