package com.pusidun.hadoop.mapreduce.topn;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * reducer端处理
 */
public class TopNReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
    // 定义一个TreeMap作为存储数据的容器（天然按key排序）
    private TreeMap<FlowBean, Text> flowMap = new TreeMap<FlowBean, Text>();
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            FlowBean bean = new FlowBean();
            bean.set(key.getDownFlow(), key.getUpFlow());
            flowMap.put(bean, new Text(value));
            if (flowMap.size() > 10) {
                flowMap.remove(flowMap.lastKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<FlowBean> bean = flowMap.keySet().iterator();
        while (bean.hasNext()){
            FlowBean v = bean.next();
            context.write(flowMap.get(v),v);
        }
    }
}
