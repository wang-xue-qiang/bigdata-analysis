package com.pusidun.netty.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import java.net.URI;


public class HttpServerHandler extends SimpleChannelInboundHandler<HttpObject> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {

        System.out.println("对应的channel=" + ctx.channel() + "\npipeline=" + ctx.pipeline() +
                "   \n通过pipeline获取的channel=" + ctx.pipeline().channel() + "\n当前ctx的handler=" + ctx.handler()+"\n");

        if(msg instanceof HttpRequest){

            System.out.println("ctx 类型="+ctx.getClass());
            System.out.println("pipeline hashcode=" + ctx.pipeline().hashCode() + " HttpServerHandler hash=" + this.hashCode());
            System.out.println("msg 类型=" + msg.getClass());
            System.out.println("客户端地址=" + ctx.channel().remoteAddress());

            //获取到
            HttpRequest httpRequest = (HttpRequest) msg;
            //获取uri, 过滤指定的资源
            URI uri = new URI(httpRequest.uri());
            if("/favicon.ico".equals(uri.getPath())) {
                System.out.println("请求了 favicon.ico, 不做响应");
                return;
            }

            //回复信息给浏览器 [http协议]
            ByteBuf content = Unpooled.copiedBuffer("Hello，我是服务器！", CharsetUtil.UTF_8);
            //构造一个httpResponse
            DefaultHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE,"text/plain");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH,content.readableBytes());
            ctx.writeAndFlush(response);
        }
    }

}
