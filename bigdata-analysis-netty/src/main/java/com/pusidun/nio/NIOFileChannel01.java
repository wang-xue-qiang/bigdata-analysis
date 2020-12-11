package com.pusidun.nio;

import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 文件写
 */
public class NIOFileChannel01 {
    public static void main(String[] args) throws Exception {
        String str = "Hello Boy!";
        //创建一个输出流
        FileOutputStream fileOutputStream = new FileOutputStream("d:\\file.txt");
        //通过 fileOutputStream 获取对应的 FileChannel
        FileChannel fileChannel = fileOutputStream.getChannel();

        //创建一个缓冲区域
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);

        //将str 放入byteBuffer
        byteBuffer.put(str.getBytes());

        //对byteBuffer进行反转
        byteBuffer.flip();

        //将byteBuffer数据写入到fileChannel
        fileChannel.write(byteBuffer);

        fileOutputStream.close();
    }
}
