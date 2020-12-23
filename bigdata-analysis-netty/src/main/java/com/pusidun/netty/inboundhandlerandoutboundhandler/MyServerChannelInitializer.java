package com.pusidun.netty.inboundhandlerandoutboundhandler;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;


public class MyServerChannelInitializer  extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {

        ChannelPipeline pipeline = ch.pipeline();


        // 入栈的handler进行编码 MyByteToLongDecoder
        pipeline.addLast(new MyByteToLongDecoder());



        //加入一个自定义的Handler, 处理业务
        pipeline.addLast(new MyServerHandler());
    }
}
