package com.wyt.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * flume 自定义输出算子
 * 由于 Source 的事务是通过信道制作的，
 * 而Sink的事务是通过自己的事务管理器来实现的，
 * 如果要实现事务，需要重写start和stop方法
 * 如果sink的事务处理结束，那么会返回信道告知消息未丢失，从而删除信道中的数据
 */
public class MySink extends AbstractSink implements Configurable {

    //定义前缀 后缀
    private String prefix;
    private String suffix;
    //创建Log对象
    private Logger logger  = LoggerFactory.getLogger(MySink.class);

    /**
     * 重写start
     */
    @Override
    public synchronized void start() {
        super.start();
    }


    /**
     * 读取配置文件
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        //读取配置文件的前后缀
        prefix = context.getString("prefix", "我是默认前缀--");
        suffix = context.getString("suffix", "--我是默认后缀");
    }

    /**
     * 拉取信息
     *
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        //1.获取信道,获取事务,开启事务
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();

        try {
            //2.从信道中获取数据
            Event event = null;
            while (true) {
                event = channel.take();
                if (Objects.nonNull(event)) {
                    //数据可能为空，抓取不到就一直抓！
                    break;
                }
            }
            //3.处理事件，此处只是打印到控制台,顺便拼接前后缀,此处为控制台打印，所以
            //执行要添加  -Dflume.root.logger=INFO,console
            logger.info(prefix + new String(event.getBody()) + suffix);
            //4.提交事务
            transaction.commit();
            //5.返回状态
            return Status.READY;
        } catch (Exception e) {
            //出现异常,回滚数据
            transaction.rollback();
            return Status.BACKOFF;
        } finally {
            transaction.close();
        }
    }


    /**
     * 重写stop
     */
    @Override
    public synchronized void stop() {
        super.stop();
    }


}
