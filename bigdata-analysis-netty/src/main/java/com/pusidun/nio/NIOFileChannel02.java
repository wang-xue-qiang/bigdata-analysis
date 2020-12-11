package com.pusidun.nio;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 文件读
 */
public class NIOFileChannel02 {
    public static void main(String[] args) throws Exception {

        //创建文件的输入流
        File file = new File("d:\\file.txt");
        FileInputStream fileInputStream = new FileInputStream(file);

        //通过fileInputStream 获取fileChannel
        FileChannel fileChannel = fileInputStream.getChannel();

        //创建缓冲区
        ByteBuffer byteBuffer = ByteBuffer.allocate((int) file.length());

        //将通道的数据读到缓冲区
        fileChannel.read(byteBuffer);

        //将byteBuffer的字节数据转成string
        System.out.println(new String(byteBuffer.array()));

        fileInputStream.close();

    }
}
