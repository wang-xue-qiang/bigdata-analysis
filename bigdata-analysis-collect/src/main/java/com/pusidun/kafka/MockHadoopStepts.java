package com.pusidun.kafka;

import com.pusidun.utils.InfoUtils;

import java.io.File;
import java.io.FileOutputStream;

/**
 * 该数据用于Hadoop测试串行jobs作业
 */
public class MockHadoopStepts {

    public static void main(String[] args) {
        genFile();
    }
    public static void genFile(){
        File file = new File("./stepts");
        if(!file.exists()){
            file.mkdir();
        }

        for (int j = 0; j < 4 ; j++) {
            try {
                String fileName = "./stepts/"+j+".txt";
                FileOutputStream fos = new FileOutputStream(fileName,false);
                for (int i = 1; i <= 10000 ; i++) {
                    String msg =  InfoUtils.getSearchWords() +"\n";
                    fos.write(msg.getBytes());
                }
                fos.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
