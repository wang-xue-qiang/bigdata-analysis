package com.pusidun.kafka;

import com.pusidun.utils.InfoUtils;

import java.io.FileOutputStream;

/**
 * 生成Hadoop中topN数据
 */
public class MockTopN {

    /**
     * 运行入口
     * @param args
     */
    public static void main(String[] args) {
        genFileData();
    }

    /**
     * 生成文件
     */
    private static void genFileData(){
        try {
            FileOutputStream fos = new FileOutputStream("./topN.txt",true);
            for (int i = 1; i <= 10000 ; i++) {
                String phone = InfoUtils.getTel();
                long upFlow = InfoUtils.getNum(1000,10000);
                long downFlow = InfoUtils.getNum(1000,10000);
                long sumFlow = upFlow + downFlow;
                String msg = phone+"\t"+upFlow+"\t"+downFlow+"\t"+sumFlow+"\n";
                fos.write(msg.getBytes());
            }
            fos.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
