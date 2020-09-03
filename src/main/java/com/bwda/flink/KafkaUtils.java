package com.bwda.flink;

import com.alibaba.fastjson.JSON;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaUtils {
    public static final String broker_list = "192.168.2.41:9092";
    public static final String topic = "test";  // kafka topic，Flink 程序中需要和这个统一

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        Metric metric = new Metric();
        metric.setTimestamp(System.currentTimeMillis());
        metric.setName("mem");
        Map<String, String> tags = new HashMap<String, String>();
        Map<String, Object> fields = new HashMap<String, Object>();


        fields.put("flowId", 90d);
        fields.put("requestTime", 27244873d);

      /*  metric.setTags(tags);
        metric.setFields(fields);*/

        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(fields));
        producer.send(record);
        System.out.println("发送数据: " + JSON.toJSONString(fields));

    }

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(300);
            writeToKafka();
        }
    }
}
