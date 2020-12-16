package com.pusidun.netty.http;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpServerCodec;

/**
 * http 交互
 */
public class HttpServer {

    public static void main(String[] args) throws  Exception{

        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        try{
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup,workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            //得到管道
                            ChannelPipeline pipeline = ch.pipeline();

                            //加入一个netty 提供的httpServerCodec codec =>[coder - decoder]
                            //HttpServerCodec 说明
                            //1. HttpServerCodec 是netty 提供的处理http的 编-解码器
                            pipeline.addLast("HttpServerCodec",new HttpServerCodec());
                            //2. 增加一个自定义的handler
                            pipeline.addLast("HttpServerHandler", new HttpServerHandler());

                            System.out.println("ok~~~~");
                        }
                    });

            ChannelFuture channelFuture = bootstrap.bind(8080).sync();
            channelFuture.channel().closeFuture().sync();

        }finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }

    }

}
