package com.wyt.fliter;

import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Map;

/***
 * 自定义拦截器
 */
public class TypeInterceptor implements Interceptor {
    //header中的key
    private static final String key = "type";
    //判断value
    private static final String dataValue = "wyt";
    //存放处理后的事件
    private static List<Event> eventList = Lists.newArrayList();
    /**
     * 初始化
     */
    @Override
    public void initialize() {

    }

    /**
     * 单个事件处理方法 当body中包含wyt时，将type设置为wyt，否则设置为other
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        //头信息
        Map<String, String> headers = event.getHeaders();
        //获取body
        String body = new String(event.getBody());
        //解析信息
        if(body.contains(dataValue)){
            headers.put(key,"wyt");
        }else{
            headers.put(key,"other");
        }
        //返回数据
        return event;
    }

    /**
     * 批量事件处理方法
     * @param list
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> list) {
        //清空集合
        eventList.clear();
        //遍历
        for (Event event:list) {
            eventList.add(intercept(event));
        }
        return eventList;
    }

    /**
     * 关闭资源
     */
    @Override
    public void close() {

    }

    /**
     * 建造者模式
     */
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
