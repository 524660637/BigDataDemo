package com.bwda.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

public class ElasticSearchSinkUtil {

    public static void main(String[] args) throws Exception {
        //读取kafka数据
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.2.41:9092");
        props.put("zookeeper.connect", "192.168.2.41:2181");
        props.put("group.id", "test");

        FlinkKafkaConsumer09<String> myConsumer = new FlinkKafkaConsumer09<String>("test",new SimpleStringSchema(), props);
        DataStream<String> stream = env.addSource(myConsumer);
        DataStream<Tuple2<String, Integer>> counts = stream.flatMap(new ElasticSearchSinkUtil.LineSplitter()).keyBy(0).sum(1);
        counts.print();

        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", "elsearch");
//该配置表示批量写入ES时的记录条数
        config.put("bulk.flush.max.actions", "1");

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        //transportAddresses.add(new InetSocketAddress(InetAddress.getByName("0.0.0.0"), 9300));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.2.26"), 9300));

        stream.addSink(new ElasticsearchSink<>(config, transportAddresses, new ElasticsearchSinkFunction<String>() {

            public IndexRequest createIndexRequest(String element) {
                Map<String, String> json = new HashMap<>();
                //将需要写入ES的字段依次添加到Map当中
                json.put("data", element);

                return Requests.indexRequest()
                        .index("flink")
                        .type("count")
                        .source(json);
            }

            public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(createIndexRequest(element));
            }
        }));



        env.execute("Save to Es data");
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }


}
