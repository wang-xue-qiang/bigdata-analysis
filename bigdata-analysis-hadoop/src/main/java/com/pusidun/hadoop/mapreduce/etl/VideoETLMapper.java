package com.pusidun.hadoop.mapreduce.etl;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 视频数据进行切分
 * yarn jar videoETL.jar /videoETL/video /videoETL/video-filter
 */
public class VideoETLMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    Text text = new Text();

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String etlString = ETLUtil.oriString2ETLString(value.toString());
        if (StringUtils.isBlank(etlString)) return;
        text.set(etlString);
        context.write(NullWritable.get(), text);
    }

}