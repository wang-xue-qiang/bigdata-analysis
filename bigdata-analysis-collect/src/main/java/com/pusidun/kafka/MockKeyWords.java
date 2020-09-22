package com.pusidun.kafka;

import com.pusidun.utils.InfoUtils;
import com.pusidun.utils.KafkaProducer;
import java.io.FileOutputStream;

/**
 * 该数据用于Flink测试用户喜爱搜索关键字
 */
public class MockKeyWords {

    /**
     * 运行入口
     * @param args 参数
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        genFileData();
    }

    /**
     * 生成文件
     */
    private static void genFileData(){
        try {
            FileOutputStream fos = new FileOutputStream("./keywords.txt",true);
            for (int i = 1; i <= 10000000 ; i++) {
                //String msg = String.format("%010d", InfoUtils.getNum(1,10000)) + "\t" + InfoUtils.getSearchWords() +"\n";
                String uid = String.format("%010d", InfoUtils.getNum(1,10000));
                String keyWord = InfoUtils.getSearchWords();
                long time = System.currentTimeMillis();
                String url = InfoUtils.getURL();
                String msg = i+"\t"+uid+"\t"+keyWord+"\t"+time+"\t"+url+"\n";
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
            String topic = "topic-keyword";
            while (true) {
                String msg = String.format("%010d", InfoUtils.getNum(1,10000)) + "\t" + InfoUtils.getSearchWords();
                System.out.println(msg);
                producer.sendMessgae(topic, msg);
                Thread.sleep(1000);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
