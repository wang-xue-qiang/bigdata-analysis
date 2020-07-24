package com.pusidun.kafka;

import com.pusidun.utils.InfoUtils;

import java.io.FileOutputStream;

/**
 * 该数据用于Hadoop测试wordCount程序
 */
public class MockWordCount {
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
            FileOutputStream fos = new FileOutputStream("./wordCount.txt",true);
            for (int i = 1; i <= 10000 ; i++) {
                String msg = InfoUtils.getSearchWords()+"\t"+InfoUtils.getSearchWords()+"\n";
                fos.write(msg.getBytes());
            }
            fos.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
