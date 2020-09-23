package com.pusidun.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 日志分类
 */
public class LogTypeInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        //1. 获取body数据
        byte[] body = event.getBody();
        String log = new String(body,Charset.forName("UTF-8"));
        //2. 获取header
        Map<String, String> headers = event.getHeaders();
        //3. 向header 添加值
        if(log.contains("start")){
            headers.put("topic","topic_start");
        }else{
            headers.put("topic","topic_event");
        }
        return event;
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
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
