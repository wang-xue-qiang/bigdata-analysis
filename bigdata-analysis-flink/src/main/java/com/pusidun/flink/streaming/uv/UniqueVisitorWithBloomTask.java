package com.pusidun.flink.streaming.uv;

import com.pusidun.utils.DateUtils;
import com.pusidun.utils.JedisUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 使用Bloom过滤器统计每小时活跃人数
 */
public class UniqueVisitorWithBloomTask {

    public static void main(String[] args) throws Exception {
        //1.构建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //只有开启了CheckPointing,才会有重启策略
        env.enableCheckpointing(5000);
        //此处设置重启策略为：出现异常重启3次，隔10秒一次
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
        //系统异常退出或人为 Cancel 掉，不删除checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置Checkpoint模式（与Kafka整合，一定要设置Checkpoint模式为Exactly_Once）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String topic = "test-003";
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1.com:9092,node2.com:9092,node3.com:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //如果没有记录偏移量，第一次从最开始消费earliest、latest
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        //Kafka的消费者，不自动提交偏移量
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        SingleOutputStreamOperator<Map> apply =
                env.addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties))
                        .map(new MapFunction<String, UniqueVisitorEntity>() {
                            @Override
                            public UniqueVisitorEntity map(String s) throws Exception {
                                String[] strings = s.split("\t");
                                UniqueVisitorEntity entity = new UniqueVisitorEntity();
                                entity.setUid(strings[0]);
                                entity.setEventTime(Long.parseLong(strings[1]));
                                return entity;
                            }
                        })
                        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UniqueVisitorEntity>(Time.seconds(5)) {
                            @Override
                            public long extractTimestamp(UniqueVisitorEntity element) {
                                return element.getEventTime();
                            }
                        })
                .keyBy("uid")
                .timeWindow(Time.hours(1))
                .trigger(new Trigger<UniqueVisitorEntity, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(UniqueVisitorEntity element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        // 每来一条数据，就直接触发窗口操作，并清空所有窗口状态
                        return TriggerResult.FIRE_AND_PURGE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

                    }
                })
                .process(new ProcessWindowFunction<UniqueVisitorEntity, Map, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<UniqueVisitorEntity> elements, Collector<Map> out) throws Exception {
                        //结果定义
                        Map<String,Object> resultMap = new HashMap<>();
                        long count = 0;
                        //位图存储方式 key是windowEnd,value是bitmap
                        long endTime = context.window().getEnd();
                        String countStr = JedisUtils.getInstance().hget("game_uv_count", endTime + "");
                        System.out.println("============="+countStr);
                        if(StringUtils.isNotEmpty(countStr)){
                            count = Long.parseLong(countStr);
                        }
                        String key = tuple.getField(0);
                        System.out.println("============="+key);
                        long offSet = bloomHash(1<<29,key,67);
                        boolean isExist = JedisUtils.getInstance().getbit("game_uv_bitmap", offSet);
                        //位图中是否存在
                        if(!isExist){
                            count = count +1;
                            JedisUtils.getInstance().setbit("game_uv_bitmap",offSet,true);
                            resultMap.put("activeUser",count);
                            resultMap.put("time",DateUtils.longToDate(endTime));
                            out.collect(resultMap);
                            JedisUtils.getInstance().hset("game_uv_count", endTime + "",count+"");
                        }else{
                            resultMap.put("activeUser",count);
                            resultMap.put("time",DateUtils.longToDate(endTime));
                            out.collect(resultMap);
                        }
                    }
                });
        //sink输出到es
        apply.print();

        apply.print();
        env.execute();
    }


    /**
     * 自定义Bloom过滤器用于压缩空间
     * @param size 容器大小
     * @param value 值
     * @param seed  随机种子
     * @return 返回值
     */
    private  static long bloomHash(long size,String value,int seed){
        long result = 0;
        long cap = 0 ;
        if(size >0 ){
            cap = size;
        }else{
            cap =  1<< 27;
        }

        for(int i = 0 ; i< value.length() ; i++){
            result =  result * seed + value.charAt(i);
        }
        return  result & (cap - 1);
    }

}
