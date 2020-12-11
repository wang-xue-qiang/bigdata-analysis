package com.pusidun.nio;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;

/**
 * 非阻塞NIO
 */
public class NIOServer {
    public static void main(String[] args) throws  Exception {

        //创建ServerSocketChannel > ServerSocket
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();

        //得到一个Selector对象
        Selector selector = Selector.open();

        //绑定一个端口，在服务器端监听
        serverSocketChannel.socket().bind(new InetSocketAddress(6666));
        //设置为非阻塞
        serverSocketChannel.configureBlocking(false);

        //把 serverSocketChannel 注册到  selector  事件为 OP_ACCEPT
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        System.out.println("SelectionKey 数量=" + selector.keys().size()); // 1

        //循环等待客户端连接
        while (true){

            //等待1秒，如果没有事件发生返回
            if(selector.select(1000) == 0){
                System.out.println("服务器等待了1秒，无连接...");
                continue;
            }

            //如果返回的>0, 就获取到相关的 selectionKey集合
            //1.如果返回的>0， 表示已经获取到关注的事件
            //2. selector.selectedKeys() 返回关注事件的集合
            //   通过 selectionKeys 反向获取通道
            Set<SelectionKey> selectionKeys = selector.selectedKeys();
            System.out.println("selectionKeys 数量 = " + selectionKeys.size());

            //遍历 Set<SelectionKey>, 使用迭代器遍历
            Iterator<SelectionKey> keyIterator = selectionKeys.iterator();
            while (keyIterator.hasNext()) {

                //获取到SelectionKey
                SelectionKey key = keyIterator.next();

                //根据key 对应的通道发生的事件做相应处理
                if(key.isAcceptable()){//如果是 OP_ACCEPT, 有新的客户端连接

                    ///该该客户端生成一个 SocketChannel
                    SocketChannel socketChannel = serverSocketChannel.accept();
                    System.out.println("客户端连接成功 生成了一个 socketChannel " + socketChannel.hashCode());

                    //将  SocketChannel 设置为非阻塞
                    socketChannel.configureBlocking(false);

                    //将socketChannel 注册到selector, 关注事件为 OP_READ， 同时给socketChannel
                    socketChannel.register(selector,SelectionKey.OP_READ, ByteBuffer.allocate(1024));

                    System.out.println("客户端连接后 ，selectionKeys 数量=" + selector.keys().size()); //2,3,4..
                }

                //发生 OP_READ
                if(key.isReadable()){
                    //通过key 反向获取到对应channel
                    SocketChannel channel = (SocketChannel)key.channel();

                    ByteBuffer buffer = (ByteBuffer)//获取到该channel关联的buffer
                    key.attachment();
                    channel.read(buffer);
                    System.out.println("form 客户端 " + new String(buffer.array()));
                }

                //手动从集合中移动当前的selectionKey, 防止重复操作
                keyIterator.remove();
            }



        }


    }
}
