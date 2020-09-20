package com.pusidun.hadoop.mapreduce.etl;

/**
 * 工具类
 */
public class ETLUtil {

    /**
     * 原始数据过滤
     *
     * @param ori 原始字符串
     * @return 过滤后字符串
     */
    public static String oriString2ETLString(String ori) {
        StringBuilder etlString = new StringBuilder();
        String[] splits = ori.split("\t");
        if (splits.length < 9) return null;
        splits[3] = splits[3].replace(" ", "");
        for (int i = 0; i < splits.length; i++) {
            if (i < 9) {
                if (i == splits.length - 1) {
                    etlString.append(splits[i]);
                } else {
                    etlString.append(splits[i] + "\t");
                }
            } else {
                if (i == splits.length - 1) {
                    etlString.append(splits[i]);
                } else {
                    etlString.append(splits[i] + "&");
                }
            }
        }
        return etlString.toString();
    }
}
