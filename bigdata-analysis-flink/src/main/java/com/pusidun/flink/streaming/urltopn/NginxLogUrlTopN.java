package com.pusidun.flink.streaming.urltopn;

import com.pusidun.utils.EsClientUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Timestamp;
import java.util.*;

/**
 * 统计每小时内nginx日志中访问前十的url
 */
public class NginxLogUrlTopN {

    /**
     * 程序入口
     * @param args 参数
     * @throws Exception e
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

        String topic = "test-top10";
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "bigdata-node01.com:9092,bigdata-node02.com:9092,bigdata-node03.com:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //如果没有记录偏移量，第一次从最开始消费earliest、latest
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        //Kafka的消费者，不自动提交偏移量
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        SingleOutputStreamOperator<String> process =
            //env.readTextFile(filePath)
            env.addSource(new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties))
            .process(new ProcessFunction<String, ApacheLogEvent>() {
                @Override
                public void processElement(String line, Context ctx, Collector<ApacheLogEvent> out) throws Exception {
                    String[] dataArray = line.split(" ");
                    ApacheLogEvent logEvent = new ApacheLogEvent(Long.parseLong(dataArray[1]), dataArray[2]);
                    out.collect(logEvent);
                }
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(5)) {
                @Override
                public long extractTimestamp(ApacheLogEvent element) {
                    return element.getEventTime();
                }
            })
            .keyBy("url")
            .timeWindow(Time.hours(1),Time.minutes(5))
            .aggregate(new UrlCountAgg(), new WindowResult())
            .keyBy("windowEnd")
            .process(new KeyedProcessFunction<Tuple, ApacheUrlCount, String>() {
                //定义一个状态
                private  transient ValueState<List<ApacheUrlCount>> valueState;
                @Override
                public void open(Configuration parameters) throws Exception {
                    ValueStateDescriptor<List<ApacheUrlCount>> valueStateDescriptor =  new ValueStateDescriptor<List<ApacheUrlCount>>
                            ("list-state",TypeInformation.of(new TypeHint<List<ApacheUrlCount>>() {}));
                    valueState = getRuntimeContext().getState(valueStateDescriptor);
                }
                //处理每一个元素
                @Override
                public void processElement(ApacheUrlCount value, Context ctx, Collector<String> out) throws Exception {
                    //获取状态值
                    List<ApacheUrlCount> list = valueState.value();
                    if(null == list){
                        list = new ArrayList<ApacheUrlCount>();
                    }
                    list.add(value);
                    valueState.update(list);
                    //注册定时器,当为窗口最后的时间时，通过加1触发定时器
                    ctx.timerService().registerEventTimeTimer(value.getWindowEnd()+1);
                }

                //定时处理，排序操作取N
                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                    List<ApacheUrlCount> list = valueState.value();
                    list.sort(new Comparator<ApacheUrlCount>() {
                        public int compare(ApacheUrlCount o1, ApacheUrlCount o2) {
                            return -(int)(o1.getCount() - o2.getCount());
                        }
                    });

                    StringBuffer result = new StringBuffer();
                    result.append("时间：").append( new Timestamp( timestamp - 1 ) ).append("\n");
                    for (int i = 0; i < 10; i++) {
                        result.append("NO").append(i + 1).append(":")
                                .append(" URL=").append(list.get(i).getUrl())
                                .append(" 访问量=").append(list.get(i).getCount()).append("\n");
                    }
                    valueState.update(null);
                    out.collect(result.toString());

                    //将结果写入ElasticSearch
                    Map<String, Object> resultMap = new HashMap<>();
                    resultMap.put("data", result.toString());
                    resultMap.put("create_time", new Date(timestamp - 1 ));
                    EsClientUtils.getInstance().addIndexMap("url_hours_count","url",resultMap);
                }
            });
        process.print();
        env.execute();
    }

    /**
     * 自定义聚合函数 AggregateFunction<输入的类型,累加器的类型,输出的数据类型></>
     */
    public static class UrlCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        public Long createAccumulator() {
            return 0l;
        }
        public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
            return aLong + 1;
        }
        public Long getResult(Long aLong) {
            return aLong;
        }
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    /**
     * 自定义聚合输出结果 WindowFunction<聚合函数输出类型,输出类型,分组类型,窗口对象></>
     */
    public static class WindowResult implements WindowFunction<Long, ApacheUrlCount, Tuple, TimeWindow> {
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ApacheUrlCount> out) throws Exception {
            String url = tuple.getField(0);
            Long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ApacheUrlCount(url, windowEnd, count));
        }
    }


}
