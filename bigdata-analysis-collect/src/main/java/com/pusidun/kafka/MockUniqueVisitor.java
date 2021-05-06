package com.pusidun.kafka;

import com.pusidun.utils.InfoUtils;
import com.pusidun.utils.KafkaProducer;
import java.io.FileOutputStream;
import java.util.Date;

/**
 * 该数据用于Flink实时统计每小时活跃人数
 */
public class MockUniqueVisitor {

    /**
     * 运行入口
     * @param args 参数
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        genKafakData();
    }


    /**
     * 生成文件数据
     */
    private static void genFileData(){
        try {
            FileOutputStream fos = new FileOutputStream("./uv.txt",true);
            for (int i = 1; i <= 10000 ; i++) {
                String msg = String.format("%010d", InfoUtils.getNum(1, 10000))
                        + "\t" + new Date().getTime()+"\n";
                fos.write(msg.getBytes());
            }
            fos.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 生成kafka相关数据
     */
    private static void genKafakData(){
        try {
            KafkaProducer producer = KafkaProducer.getInstance();
            String topic = "test";
            while (true) {
                String msg = String.format("%010d", InfoUtils.getNum(1, 10000))
                        + "\t" + new Date().getTime();
                System.out.println(msg);
                producer.sendMessgae(topic, "{\"common\":{\"ar\":\"310000\",\"ba\":\"iPhone\",\"ch\":\"Appstore\",\"md\":\"iPhone X\",\"mid\":\"mid_3\",\"os\":\"iOS 13.2.9\",\"uid\":\"461\",\"vc\":\"v2.1.132\"},\"start\":{\"entry\":\"notice\",\"loading_time\":7457,\"open_ad_id\":13,\"open_ad_ms\":6054,\"open_ad_skip_ms\":0},\"ts\":1609999615000}\n");
                Thread.sleep(1000);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
