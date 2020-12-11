package com.pusidun.nio;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * MappedByteBuffer
 * 1.可让文件直接再内存(堆外内存)修改，操作系统不需copy
 */
public class MappedByteBufferTest {
    public static void main(String[] args) throws Exception {

        RandomAccessFile randomAccessFile = new RandomAccessFile("d:\\hello.txt", "rw");

        //获取对应的通道
        FileChannel channel = randomAccessFile.getChannel();

        /**
         * 参数1：使用读写模式
         * 参数2：可以直接修改的起始位置
         * 参数3：映射到内存的大小
         */
        MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 5);

        mappedByteBuffer.put(0,(byte) 'H');
        mappedByteBuffer.put(3,(byte) '9');

        randomAccessFile.close();
    }
}
