package com.pusidun.hadoop.mapreduce.steps;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 多job串行处理
 */
public class OneIndexMapper extends Mapper<LongWritable, Text,Text, LongWritable> {

    LongWritable v = new LongWritable(1);
    Text k = new Text();
    String fileName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit)context.getInputSplit();
        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(null != value) {
            String lineValue = value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
            while (stringTokenizer.hasMoreTokens()) {
                String str = stringTokenizer.nextToken();
                k.set(str+"--"+fileName);
                context.write(k, v);
            }
        }
    }
}
