package com.pusidun.netty.websocket;


import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

public class WebSocketServer {
    public static void main(String[] args) throws Exception{
        NioEventLoopGroup boosGroup = new NioEventLoopGroup();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        try{
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(boosGroup,workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            //因为基于Http的协议，使用http的编码和解码
                            pipeline.addLast(new HttpServerCodec());
                            //是以块方式写，添加ChunkedWriteHandler处理器
                            pipeline.addLast(new ChunkedWriteHandler());

                            /**
                             说明
                             1. http数据在传输过程中是分段, HttpObjectAggregator ，就是可以将多个段聚合
                             2. 这就就是为什么，当浏览器发送大量数据时，就会发出多次http请求
                             */
                            pipeline.addLast(new HttpObjectAggregator(8 * 1024));
                            /**
                            1. 对应websocket ，它的数据是以 帧(frame) 形式传递
                            2. 可以看到WebSocketFrame 下面有六个子类
                            3. 浏览器请求时 ws://localhost:8080/socket 表示请求的uri
                            4. WebSocketServerProtocolHandler 核心功能是将 http协议升级为 ws协议 , 保持长连接
                            5. 是通过一个 状态码 101
                             */
                            pipeline.addLast(new WebSocketServerProtocolHandler("/socket"));
                            //自定义的handler ，处理业务逻辑
                            pipeline.addLast(new WebSocketFrameHandler());
                        }
                    });
            ChannelFuture channelFuture = serverBootstrap.bind(8080).sync();
            channelFuture.channel().closeFuture().sync();

        }finally {
            workerGroup.shutdownGracefully();
            boosGroup.shutdownGracefully();
        }
    }
}
