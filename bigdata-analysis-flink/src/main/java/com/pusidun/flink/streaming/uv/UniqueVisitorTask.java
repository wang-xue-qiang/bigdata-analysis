package com.pusidun.flink.streaming.uv;

import com.pusidun.utils.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.*;

/**
 * 统计每小时活跃人数
 */
public class UniqueVisitorTask {

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

        String topic = "topic-s3-launch";
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
                //env.addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties))
                  env.readTextFile("./uv.txt")
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
                .timeWindowAll(Time.hours(1))
                .apply(new UVCount());

        //sink输出到es
        apply.print();

        env.execute();

    }

    /**
     * 聚合uid去重
     */
    public static class UVCount implements AllWindowFunction<UniqueVisitorEntity,Map,TimeWindow> {
        Set<String> set = new HashSet<>();
        @Override
        public void apply(TimeWindow window, Iterable<UniqueVisitorEntity> values, Collector<Map> out) throws Exception {
            Iterator<UniqueVisitorEntity> iterator = values.iterator();
            while (iterator.hasNext()){
                UniqueVisitorEntity entity = iterator.next();
                set.add(entity.getUid());
            }
            Map<String,Object> resultMap = new HashMap<>();
            resultMap.put("activeUser",set.size());
            resultMap.put("time",DateUtils.longToDate(window.getEnd()));
            set.clear();
            out.collect(resultMap);
        }
    }

}
