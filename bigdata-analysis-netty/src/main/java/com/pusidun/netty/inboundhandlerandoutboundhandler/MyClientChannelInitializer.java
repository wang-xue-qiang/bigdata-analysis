package com.pusidun.netty.inboundhandlerandoutboundhandler;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;


public class MyClientChannelInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {

        ChannelPipeline pipeline = ch.pipeline();


        // 加入一个出栈的handler进行编码 MyLongToByteEncoder
        pipeline.addLast(new MyLongToByteEncoder());

        //加入一个自定义的Handler, 处理业务
        pipeline.addLast(new MyClientHandler());

    }
}
