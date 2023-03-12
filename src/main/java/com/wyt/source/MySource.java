package com.wyt.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;
import java.util.Map;

/**
 * flume - 自定义数据源
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {

    //定义配置文件将来要读取的字段
    private Long delay;
    private String field;
    private Map<String, String> headers;

    /**
     * 初始化信息，读取配置文件的值
     * @param context
     */
    @Override
    public void configure(Context context) {
        delay = context.getLong("delay");
        field = context.getString("field", "Hello!");
        //创建请求头
        headers = new HashMap<>();
        headers.put("type", "wyt");
    }


    @Override
    public Status process() throws EventDeliveryException {
        try{

            //创建事件
            SimpleEvent event = new SimpleEvent();
            //创建一个假的数据源
            for (int i = 0; i < 5; i++) {
                //请求头
                event.setHeaders(headers);
                //请求体
                event.setBody((field + i).getBytes());
                //将事件写入 channel
                getChannelProcessor().processEvent(event);
                Thread.sleep(delay);
            }
        }catch (Exception e){
            e.printStackTrace();
            return Status.BACKOFF;
        }
        return Status.READY;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }


}
