package com.bwda.flume.custom;

import com.google.common.base.Preconditions;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

public class CustomSink extends AbstractSink implements Configurable {
    private String hdfsURI;

    private String username;

    private String dataDir;

    private String dateFormat;

    //数据处理的逻辑都在process方法中实现
    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction ts = channel.getTransaction();

        //event就是自定义Sink中接受的数据
        Event event;
        ts.begin();

        while (true)
        {
            event = channel.take();
            if(event != null)
            {
                break;
            }
        }
        String eventBody = new String(event.getBody());
            ts.commit();
            ts.close();
            return Status.READY;
        }


    //该方法用于读取Flume中Sink的配置
    @Override
    public void configure(Context context)
    {
        // customelog.sinks.sink1.type=death.flume.FlumeSinkDemo
        // customelog.sinks.sink1.channel=channel1
        // customelog.sinks.sink1.hdfsURI=hdfs://hostname:port
        // customelog.sinks.sink1.username=hdfs
        // customelog.sinks.sink1.dataDir=/death/data_sampling
        // customelog.sinks.sink1.dateFormat=YYYY-MM-dd

        hdfsURI = context.getString("hdfsURI");
        Preconditions.checkNotNull(hdfsURI, "hdfsURI must be set");

        username = context.getString("username");
        Preconditions.checkNotNull(username, "username must be set");

        dataDir = context.getString("dataDir");
        Preconditions.checkNotNull("dataDir must be set");

        dateFormat = context.getString("dateFormat");
        Preconditions.checkNotNull(dateFormat, "dateFormat must be set");
    }

    //该方法用于初始化使用，即该Sink启动时调用
    @Override
    public synchronized void start()
    {
        super.start();
    }

    //该方法用于Sink停止使用调用
    @Override
    public synchronized void stop()
    {
        super.stop();
    }

}
