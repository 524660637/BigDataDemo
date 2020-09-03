package com.bwda.flume.custom;



import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;


/**
 * 自定义拦截器，实现Interceptor接口，并且实现其抽象方法
 */
public class CustomInterceptor implements Interceptor {


    //自定义拦截器参数，用来接收自定义拦截器flume配置参数
    private static String param = "";


    /**
     * 拦截器构造方法，在自定义拦截器静态内部类的build方法中调用，用来创建自定义拦截器对象。
     */
    public CustomInterceptor() {
        System.err.println("----------自定义拦截器构造方法执行");
    }


    /**
     * 该方法用来初始化拦截器，在拦截器的构造方法执行之后执行，也就是创建完拦截器对象之后执行
     */
    @Override
    public void initialize() {
        System.err.println("----------自定义拦截器的initialize方法执行");
    }

    /**
     * 用来处理每一个event对象，该方法不会被系统自动调用，一般在 List<Event> intercept(List<Event> events) 方法内部调用。
     *
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        System.err.println("----------intercept(Event event)方法执行，处理单个event");
        System.err.println("----------接收到的自定义拦截器参数值param值为：" + param);
        /*
        这里编写event的处理代码
         */
        System.err.println("------原始数据header为：" + event.getHeaders().toString());
        System.err.println("------原始数据body为：" + new String(event.getBody()));
        return event;
    }

    /**
     * 用来处理一批event对象集合，集合大小与flume启动配置有关，和transactionCapacity大小保持一致。一般直接调用 Event intercept(Event event) 处理每一个event数据。
     *
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        int i = 1;
        System.err.println("----------intercept(List<Event> events)方法执行");
        for (Event e : events) {
            System.err.println("------原始数据第" + (i++) + "行内容为：" + new String(e.getBody()));
        }

        /*
        这里编写对于event对象集合的处理代码，一般都是遍历event的对象集合，对于每一个event对象，调用 Event intercept(Event event) 方法，然后根据返回值是否为null，
        来将其添加到新的集合中。
         */
        List<Event> results = new ArrayList<>();
        Event event;
        for (Event e : events) {
            event = intercept(e);
            if (event != null) {
                results.add(event);
            }
        }
        i = 1;
        for (Event e : results) {
            System.err.println("------结果数据第" + (i++) + "行内容为：" + new String(e.getBody()));
        }

        return results;
    }

    /**
     * 该方法主要用来销毁拦截器对象值执行，一般是一些释放资源的处理
     */
    @Override
    public void close() {
        System.err.println("----------自定义拦截器close方法执行");
    }

    /**
     * 通过该静态内部类来创建自定义对象供flume使用，实现Interceptor.Builder接口，并实现其抽象方法
     */
    public static class Builder implements Interceptor.Builder {

        /**
         * 该方法主要用来返回创建的自定义类拦截器对象
         *
         * @return
         */
        @Override
        public Interceptor build() {
            System.err.println("----------自定义build方法执行");
            return new CustomInterceptor();
        }

        /**
         * 用来接收flume配置自定义拦截器参数
         *
         * @param context 通过该对象可以获取flume配置自定义拦截器的参数
         */
        @Override
        public void configure(Context context) {
            System.err.println("----------自定义configure方法执行");
            /*
            通过调用context对象的getString方法来获取flume配置自定义拦截器的参数，方法参数要和自定义拦截器配置中的参数保持一致+
             */
            param = context.getString("param");
        }
    }
}
