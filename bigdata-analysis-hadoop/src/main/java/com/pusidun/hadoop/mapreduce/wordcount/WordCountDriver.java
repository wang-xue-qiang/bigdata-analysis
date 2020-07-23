package com.pusidun.hadoop.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *  https://www.cnblogs.com/huxinga/p/6939896.html
 *  bin/hadoop jar myJars/ /wordCount/input/word.txt /wordCount/output
 */
public class WordCountDriver {
    public static void main(String[] args) throws Exception {

        //参数设置
        if(args .length != 2){
            System.err.printf("Usage: %s [generic options] <input> <output> \n","WordCountJob");
            System.exit(-1);
        }

        //获取配置
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, WordCountDriver.class.getSimpleName());
        //设置jar执行
        job.setJarByClass(WordCountDriver.class);
        //Map阶段
        job.setMapperClass(WordCountMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //Combiner
        job.setCombinerClass(WordCountReduce.class);
        //Reduce阶段
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //设置输入输出路径
        FileInputFormat.setInputPaths(job, args[0]);
        //NLineInputFormat.setNumLinesPerSplit(job,2);
        //MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,WordCountMap.class);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //执行
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
