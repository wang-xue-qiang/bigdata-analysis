package com.pusidun.netty.heartbeat;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import java.util.concurrent.TimeUnit;

public class HeartServer {
    public static void main(String[] args)  throws  Exception{

        //创建两个线程组
        NioEventLoopGroup bossGroup = new NioEventLoopGroup();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();

        try{

            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup,workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {

                            ChannelPipeline pipeline = ch.pipeline();
                            //加入一个netty提供的IdleStateHandler
                            /**
                             *  1. IdleStateHandler 是netty 提供的处理空闲状态的处理器
                             *  2. long readerIdleTime : 表示多长时间没有读, 就会发送一个心跳检测包检测是否连接
                             *  3. long writerIdleTime : 表示多长时间没有写, 就会发送一个心跳检测包检测是否连接
                             *  4. long allIdleTime : 表示多长时间没有读写, 就会发送一个心跳检测包检测是否连接
                             *  5. 文档说明
                             *   triggers an {@link IdleStateEvent} when a {@link Channel} has not performed
                             *   read, write, or both operation for a while.
                             *  6. 当 IdleStateEvent 触发后 , 就会传递给管道 的下一个handler去处理
                             *  通过调用(触发)下一个handler 的 userEventTiggered , 在该方法中去处理 IdleStateEvent(读空闲，写空闲，读写空闲)
                             *
                             */
                            pipeline.addLast(new IdleStateHandler(3,5,10, TimeUnit.SECONDS));
                            pipeline.addLast( new HeartServerHandler());

                        }
                    });
            ChannelFuture channelFuture = serverBootstrap.bind(8080).sync();
            channelFuture.channel().closeFuture().sync();

        }finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();

        }
    }
}
