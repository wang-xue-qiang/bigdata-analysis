package com.pusidun.flink.streaming.brandlike;

import com.alibaba.fastjson.JSONObject;
import com.pusidun.utils.DateUtils;
import com.pusidun.utils.HBaseUtils;
import com.pusidun.utils.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 统计用户喜欢的品牌
 * get 'shop:user', '0000000002'
 */
public class BrandLikeTask {

    /**
     * 程序入口
     * @param args 参数
     * @throws Exception
     */
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

        String topic = "test-001";
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1.com:9092,node2.com:9092,node3.com:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group2");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //如果没有记录偏移量，第一次从最开始消费earliest、latest
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        //Kafka的消费者，不自动提交偏移量
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


        //2.添加dataSource
        SingleOutputStreamOperator<BrandLikeEntity> reduce =
                //env.addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties))
                  env.readTextFile("./brandLike.txt")
                        .process(new ProcessFunction<String, ScanProductEntity>() {
                            @Override
                            public void processElement(String s, Context ctx, Collector<ScanProductEntity> out) throws Exception {
                                String[] dataArray = s.split("\t");
                                ScanProductEntity productEntity = new ScanProductEntity();
                                productEntity.setUid(dataArray[0]);
                                productEntity.setUseType(Integer.parseInt(dataArray[1]));
                                productEntity.setBrand(dataArray[2]);
                                productEntity.setProductId(Integer.parseInt(dataArray[3]));
                                productEntity.setEventTime(Long.parseLong(dataArray[4]));
                                out.collect(productEntity);
                            }
                        })
                        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ScanProductEntity>(Time.seconds(5)) {
                            @Override
                            public long extractTimestamp(ScanProductEntity element) {
                                return element.getEventTime();
                            }
                        })
                        .flatMap(new FlatMapFunction<ScanProductEntity, BrandLikeEntity>() {
                            @Override
                            public void flatMap(ScanProductEntity log, Collector<BrandLikeEntity> collector) throws Exception {
                                //1.解析数据
                                String uid = log.getUid();
                                String brand = log.getBrand();
                                //2.获取之前的品牌喜好
                                String mapData = HBaseUtils.getData("shop:user", uid, "info", "brandLikeList");
                                Map<String, Integer> map = new HashMap<String, Integer>();
                                if (StringUtils.isNotBlank(mapData)) {
                                    map = JSONObject.parseObject(mapData, Map.class);
                                }
                                //获取之前的品牌偏好
                                String preMaxBrand = MapUtils.getMaxByMap(map) == null ? "" : MapUtils.getMaxByMap(map);
                                //获取当前品牌数值并更新
                                int preBrand = map.get(brand) == null ? 0 : map.get(brand);
                                map.put(brand, preBrand + 1);
                                String finalStr = JSONObject.toJSONString(map);
                                HBaseUtils.putData("shop:user", uid, "info", "brandLikeList", finalStr);
                                //获取更新后的最大值
                                String currMaxBrand = MapUtils.getMaxByMap(map);
                                if (StringUtils.isNotBlank(currMaxBrand) && !preMaxBrand.equals(currMaxBrand)) {
                                    BrandLikeEntity brandLike = new BrandLikeEntity();
                                    brandLike.setBrand(preMaxBrand);
                                    brandLike.setCount(-1);
                                    brandLike.setGroupField("brandLike==" + preMaxBrand);
                                    collector.collect(brandLike);
                                } else {
                                    BrandLikeEntity brandLike = new BrandLikeEntity();
                                    brandLike.setBrand(currMaxBrand);
                                    brandLike.setCount(1);
                                    brandLike.setGroupField("brandLike==" + currMaxBrand);
                                    collector.collect(brandLike);
                                    HBaseUtils.putData("shop:user", uid, "info", "brandLike", currMaxBrand);
                                }

                            }
                        })
                        .keyBy("groupField")
                        .timeWindow(Time.hours(1), Time.minutes(1))
                        .reduce(new ReduceFunction<BrandLikeEntity>() {
                            @Override
                            public BrandLikeEntity reduce(BrandLikeEntity a, BrandLikeEntity b) throws Exception {
                                BrandLikeEntity result = new BrandLikeEntity();
                                result.setBrand(a.getBrand());
                                result.setCount(a.getCount() + b.getCount());
                                return result;
                            }
                        });

        //添加sink
        reduce.addSink(new SinkFunction<BrandLikeEntity>() {
            @Override
            public void invoke(BrandLikeEntity value, Context context) throws Exception {

                System.err.println("水位线：" + DateUtils.longToDate(context.currentWatermark()) + "，输出结果：" + value);
            }
        });
        env.execute();

    }

    //指定数位线
    public static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<ScanProductEntity> {
        private long currentTimestamp = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
        }

        @Override
        public long extractTimestamp(ScanProductEntity element, long previousElementTimestamp) {
            this.currentTimestamp = element.getEventTime();
            return element.getEventTime();
        }
    }
}
