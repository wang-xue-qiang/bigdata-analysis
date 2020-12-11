package com.pusidun.bio;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * telnet 127.0.0.1 6666
 * ctrl+]
 * send ok100
 */
public class BIOServer {

    public static void main(String[] args) throws IOException {
        //1.创建一个线程池
        ExecutorService newCachedThreadPool = Executors.newCachedThreadPool();

        ServerSocket serverSocket = new ServerSocket(6666);

        System.out.println("服务器启动了");

        while (true) {
            //监听，等待客户端链接
            System.out.println("等待连接....");
            final Socket socket = serverSocket.accept();

            System.out.println("连接到一个客户端");

            //创建一个线程，与之通讯
            newCachedThreadPool.execute(new Runnable() {
                public void run() {
                    handler(socket);
                }

            });
        }
    }

    private static void handler(Socket socket) {
        byte[] bytes = new byte[1024];
        try {
            System.out.println("线程信息： id="+Thread.currentThread().getId()+" 名字="+Thread.currentThread().getName());
            InputStream inputStream = socket.getInputStream();
            while (true) {
                System.out.println("线程信息： id="+Thread.currentThread().getId()+" 名字="+Thread.currentThread().getName());

                System.out.println("等待read....");

                int read = inputStream.read(bytes);
                if (read != -1) {
                    System.out.println(new String(bytes, 0, read));
                } else {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
