package com.bwda.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

public class FlinkReadKafka {

    public static void main(String[] args) throws Exception {
         Arrays.asList(new String[]{"Hello", "World"}).stream().map(s-> s.split("")).   map(str -> str.toString())
                //.distinct()
                .collect(Collectors.toList()).forEach(System.out::println);
        //读取kafka数据
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.2.41:9092");
        props.put("zookeeper.connect", "192.168.2.41:2181");
        props.put("group.id", "test");

        FlinkKafkaConsumer09<String> myConsumer = new FlinkKafkaConsumer09<String>("test",new SimpleStringSchema(), props);
        DataStream<String> stream = env.addSource(myConsumer);
        DataStream<Tuple2<String, Integer>> counts = stream.flatMap(new LineSplitter()).keyBy(0).sum(1);
        counts.print();
        env.execute("WordCount from Kafka data");
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
//            String[] tokens = value.toLowerCase().split("\\W+");
//            for (String token : tokens) {
//                if (token.length() > 0) {
//                    out.collect(new Tuple2<String, Integer>(token, 1));
//                }
//            }
        }
    }
}
