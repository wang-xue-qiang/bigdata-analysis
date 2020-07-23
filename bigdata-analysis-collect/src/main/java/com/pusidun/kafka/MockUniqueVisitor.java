package com.pusidun.kafka;

import com.pusidun.utils.InfoUtils;
import com.pusidun.utils.KafkaProducer;
import java.io.FileOutputStream;
import java.util.Date;

/**
 * 生成统计UV信息
 */
public class MockUniqueVisitor {

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
            String topic = "topic-uv";
            while (true) {
                String msg = String.format("%010d", InfoUtils.getNum(1, 10000))
                        + "\t" + new Date().getTime();
                System.out.println(msg);
                producer.sendMessgae(topic, msg);
                Thread.sleep(1000);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


}
