package com.pusidun.hadoop.mapreduce.topn;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * topN在Mapper做控制
 */
public class TopNMapper extends Mapper<LongWritable, Text, FlowBean,Text> {

    FlowBean kBean ;

    // 定义一个TreeMap作为存储数据的容器（天然按key排序）
    private TreeMap<FlowBean, Text> flowMap = new TreeMap<FlowBean, Text>();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        kBean = new FlowBean();
        Text v = new Text();
        //13470253144	180	0	180
        String line = value.toString();
        String[] words = line.split("\t");
        String phoneNum = words[0];
        long upFlow = Long.parseLong(words[1]);
        long downFlow = Long.parseLong(words[2]);
        long sumFlow = Long.parseLong(words[3]);
        kBean.setDownFlow(downFlow);
        kBean.setUpFlow(upFlow);
        kBean.setSumFlow(sumFlow);
        v.set(phoneNum);
        flowMap.put(kBean,v);

        //保证只有十个kv
        if (flowMap.size()> 10){
            flowMap.remove(flowMap.lastKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<FlowBean> bean = flowMap.keySet().iterator();
        while (bean.hasNext()){
            FlowBean k = bean.next();
            context.write(k,flowMap.get(k));
        }
    }
}
