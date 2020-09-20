package com.pusidun.hadoop.mapreduce.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 驱动程序
 */
public class VideoETLDriver implements Tool {

    public static void main(String[] args)  {
        try {
            int resultCode = ToolRunner.run(new VideoETLDriver(), args);
            if(resultCode == 0){
                System.out.println("Success!");
            }else{
                System.out.println("Fail!");
            }
            System.exit(resultCode);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    private Configuration conf = null;


    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(conf);

        job.setJarByClass(VideoETLDriver.class);
        job.setMapperClass(VideoETLMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

}
