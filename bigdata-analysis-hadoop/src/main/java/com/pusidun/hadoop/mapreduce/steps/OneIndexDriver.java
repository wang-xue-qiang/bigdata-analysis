package com.pusidun.hadoop.mapreduce.steps;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 统计指定文件中每个单词出现的个数输出格式为：华为手机	0.txt-->1222	1.txt-->1213	2.txt-->1219	3.txt-->1221
 * 第一步输出结果为：
 * 华为手机--0.txt	1222
 * 华为手机--1.txt	1213
 * 华为手机--2.txt	1219
 * 华为手机--3.txt	1221
 */
public class OneIndexDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"./stepts","./one_output"};

        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input>  <output>  \n", "OneIndexDriver");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(OneIndexDriver.class);
        job.setMapperClass(OneIndexMapper.class);
        job.setReducerClass(OneIndexReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,args[0]);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 0 : 1);
    }
}
