package com.pusidun.nio;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 文件copy
 */
public class NIOFileChannel03 {
    public static void main(String[] args) throws Exception {

        //创建文件的输入流
        FileInputStream fileInputStream = new FileInputStream("d:\\hello.txt");
        FileChannel fileChannel01 = fileInputStream.getChannel();

        FileOutputStream fileOutputStream = new FileOutputStream("d:\\hello02.txt");
        FileChannel fileChannel02 = fileOutputStream.getChannel();

        //创建缓冲区
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        while (true){

            //这里一个重要的操作
            byteBuffer.clear();//标记位置重置

            int read = fileChannel01.read(byteBuffer);
            if(read == -1){
                break;
            }
            //将buffer 数据写入fileChannel02
            byteBuffer.flip();
            fileChannel02.write(byteBuffer);
        }
        fileOutputStream.close();
        fileInputStream.close();

    }
}
