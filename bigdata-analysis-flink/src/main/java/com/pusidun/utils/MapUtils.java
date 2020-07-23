package com.pusidun.utils;

import java.util.*;

public class MapUtils {

    /**
     * 使用 Map按value进行排序
     *
     * @return
     */
    public static LinkedHashMap<String, Double> sortMapByValue(Map<String, Double> oriMap) {
        if (oriMap == null || oriMap.isEmpty()) {
            return null;
        }
        LinkedHashMap<String, Double> sortedMap = new LinkedHashMap<String, Double>();
        List<Map.Entry<String, Double>> entryList = new ArrayList<Map.Entry<String, Double>>(
                oriMap.entrySet());
        Collections.sort(entryList, new MapValueComparator());

        Iterator<Map.Entry<String, Double>> iter = entryList.iterator();
        Map.Entry<String, Double> tmpEntry = null;
        while (iter.hasNext()) {
            tmpEntry = iter.next();
            sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());
        }
        return sortedMap;
    }

    /**
     * 获取最大值
     * @param dataMap
     * @return
     */
    public static String getMaxByMap(Map<String,Integer> dataMap){
        if(dataMap.isEmpty()){
            return  null;
        }
        TreeMap<Integer,String> map = new TreeMap<Integer, String>(new Comparator<Integer>() {
            public int compare(Integer o1, Integer o2) {
                return o2.compareTo(o1);
            }
        });
        Set<Map.Entry<String,Integer>> set = dataMap.entrySet();
        for(Map.Entry<String,Integer> entry :set){
            String key = entry.getKey();
            Integer value = entry.getValue();
            map.put(value,key);
        }
        return map.get(map.firstKey());
    }

    static class MapValueComparator implements Comparator<Map.Entry<String, Double>> {
        public int compare(Map.Entry<String, Double> me1, Map.Entry<String, Double> me2) {
            return me1.getValue().compareTo(me2.getValue());
        }
    }

}