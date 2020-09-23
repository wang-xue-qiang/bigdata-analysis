package com.pusidun.interceptor;

import com.pusidun.utils.LogUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * 日志简单清洗过滤
 */
public class LogETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //1. 获取数据
        byte[] body = event.getBody();
        String log = new String(body,Charset.forName("UTF-8"));
        //2.校验数据类型 启动日志（json）事件日志（服务器时间|json）
        if(log.contains("start")){
            if(LogUtils.valuateStart(log)){
                return event;
            }
        }else{
            if(LogUtils.valuateEvent(log)){
                return event;
            }
        }
        return null;
    }


    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> intercepts = new ArrayList<>();
        for (Event event : events) {
            Event intercept = intercept(event);
            if(intercept != null){
                intercepts.add(intercept);
            }
        }
        return intercepts;
    }

    @Override
    public void close() {

    }

    //创建静态内部类，提供外部使用
    public static class Builder implements  Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
