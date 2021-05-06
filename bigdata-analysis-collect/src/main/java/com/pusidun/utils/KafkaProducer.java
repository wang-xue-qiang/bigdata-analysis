package com.pusidun.utils;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * kafka生产者工具类
 */
public class KafkaProducer {


    public static Producer<String, String> producer;

    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.12.138:9092,192.168.12.139:9092,192.168.12.140:9092");
        props.put("serializer.class", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "0");
        props.put("retries", "0");
        props.put("linger.ms", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
    }

    /**
     * 向kafka发送消息
     *
     * @param message
     * @return
     */
    public void sendMessgae(ProducerRecord message) throws Exception {
        producer.send(message, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //log.info("向kafka发送数据返回偏移量: {}" , recordMetadata.offset());
            }
        });
    }


    /**
     * 向kafka发送消息
     *
     * @param topic 主题
     * @param value 值
     * @throws Exception
     */
    public void sendMessgae(String topic, String value) throws Exception {
        sendMessgae(new ProducerRecord<String, String>(topic, value));
    }

    /**
     * 向kafka发送消息
     *
     * @param topic 主题
     * @param value 值
     * @throws Exception
     */
    public void sendMessgae(String topic, String key, String value) throws Exception {
        sendMessgae(new ProducerRecord(topic, key, value));
    }

    /**
     * 刷新缓存
     */
    public void flush() {
        producer.flush();
    }


    /**
     * 关闭连接
     */
    public void close() {
        producer.close();
    }

    /**
     * 单例模式确保全局中只有一份该实例
     */
    private static class ProducerKafkaHolder {
        private static KafkaProducer instance = new KafkaProducer();
    }

    /**
     * 延迟加载，避免启动加载
     *
     * @return
     */
    public static KafkaProducer getInstance() {
        return ProducerKafkaHolder.instance;
    }


    public static void main(String[] args)  throws Exception {
        KafkaProducer producer = KafkaProducer.getInstance();
        producer.sendMessgae("test", "test");
    }

}