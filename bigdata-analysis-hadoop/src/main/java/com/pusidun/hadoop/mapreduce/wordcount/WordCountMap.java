package com.pusidun.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 数据清洗过滤操作
 * 完成 A to B 的工作
 */
public class WordCountMap extends Mapper<LongWritable, Text, Text, LongWritable> {

    LongWritable v = new LongWritable(1);
    Text k = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (null != value) {
            String lineValue = value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
            while (stringTokenizer.hasMoreTokens()) {
                String str = stringTokenizer.nextToken();
                k.set(str);
                context.write(k, v);
            }
        }
    }
}
