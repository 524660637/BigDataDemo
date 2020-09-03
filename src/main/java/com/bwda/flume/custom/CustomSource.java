package com.bwda.flume.custom;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Random;

//Flume根据数据来源的特性将Source分成两类类，像Http、netcat和exec等就是属于事件驱动型（EventDrivenSource），
// 而kafka和Jms等就是属于轮询拉取型（PollableSource）。不同的Source根据它的不同触发机制和拉取机制，在特定的时候调用ChannelProcessor来执行event的插入
//　PollableSource或者EventDrivenSource的区别在于：PollableSource是通过线程不断去调用process方法，主动拉取消息，
// 而EventDrivenSource是需要触发一个调用机制，即被动等待。
// Configurable接口：便于项目中初始化某些配置用的。
public class CustomSource extends AbstractSource implements Configurable, PollableSource,EventDrivenSource {
    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }
//在失败补偿暂停线程处理时，需要用到这个方法
    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public Status process() throws EventDeliveryException {
        Random random = new Random();
        int randomNum = random.nextInt(100);
        String text = "Hello World :" + random.nextInt(100);  //实际需要传的内容
        HashMap<String, String> header = new HashMap<String, String>();
        header.put("id", Integer.toString(randomNum));   //将id--value放入到header中
        this.getChannelProcessor().processEvent(EventBuilder.withBody(text, Charset.forName("UTF-8"), header)); //prcessEvent()将数据传上去
        return Status.READY;
    }

    @Override
    public void configure(Context arg0) {

    }
}

