package com.pusidun.nio;

import java.nio.IntBuffer;

public class BasicBuffer {
    public static void main(String[] args) {
        //创建一个buffer,大小为5，即可存5个int
        IntBuffer intBuffer = IntBuffer.allocate(5);
        for (int i = 0; i < intBuffer.capacity(); i++) {
            intBuffer.put(i * 2);
        }
        //如何从buffer读取数据
        intBuffer.flip();//将buffer转换，读写切换
        while (intBuffer.hasRemaining()) {
            System.out.println(intBuffer.get());
        }

    }
}
