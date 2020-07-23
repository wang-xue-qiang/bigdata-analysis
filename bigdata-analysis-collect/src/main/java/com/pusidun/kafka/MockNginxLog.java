package com.pusidun.kafka;

import com.pusidun.utils.InfoUtils;
import com.pusidun.utils.KafkaProducer;
import java.io.FileOutputStream;
import java.util.Date;

/**
 * 模拟生成 nginx日志
 */
public class MockNginxLog {

    /**
     * 运行入口
     * @param args 参数
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        genFileData();
    }

    /**
     * 生成文件数据
     */
    private static void genFileData(){
        try {
            FileOutputStream fos = new FileOutputStream("./nginxlog.txt",true);
            for (int i = 1; i <= 10000 ; i++) {
                String msg = "127.0.0.1"+" " + new Date().getTime()+" "+InfoUtils.getURL()+"\n";
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
            String topic = "topic-nginxlog";
            while (true) {
                String msg = "127.0.0.1"+" " + new Date().getTime()+" "+InfoUtils.getURL();
                System.out.println(msg);
                producer.sendMessgae(topic, msg);
                Thread.sleep(1000);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
