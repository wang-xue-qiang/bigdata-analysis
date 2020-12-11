package com.pusidun.nio;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 文件copy
 */
public class NIOFileChannel04 {
    public static void main(String[] args) throws Exception {

        //创建文件的输入流
        FileInputStream fileInputStream = new FileInputStream("d:\\hello.txt");
        FileChannel sourceCh = fileInputStream.getChannel();

        FileOutputStream fileOutputStream = new FileOutputStream("d:\\hello03.txt");
        FileChannel destCh = fileOutputStream.getChannel();

        destCh.transferFrom(sourceCh,0,sourceCh.size());

        sourceCh.close();
        destCh.close();
        fileOutputStream.close();
        fileInputStream.close();

    }
}
