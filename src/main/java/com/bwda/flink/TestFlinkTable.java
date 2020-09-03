package com.bwda.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class TestFlinkTable {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.enableCheckpointing(5000);
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        tableEnv.connect(
                new Kafka()
                        .version("0.9")
                        .topic("test")
                        .property("zookeeper.connect", "192.168.2.41:2181")
                        .property("bootstrap.servers", "192.168.2.41:9092"))
                .withFormat(new Json().deriveSchema())
                .withSchema(
                        new Schema()
                                .field("flowId", Types.STRING())
                                .field("requestTime",Types.STRING()))
                .inAppendMode()
                .registerTableSource("sm_user");



          Table a= tableEnv.sqlQuery("select * from sm_user").distinct();
          tableEnv.toRetractStream(a,Row.class).print();
          env.execute();
            //tableEnv.toAppendStream(a,Row.class).print();



    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word : s.split(" ")) {
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}
