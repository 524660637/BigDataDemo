package com.bwda.storm.WordCount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class LocalSumStormAckerTopology {


    public static class DataSourceSpout extends BaseRichSpout{

        private SpoutOutputCollector collector;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        int number = 0;
        @Override
        public void nextTuple() {
            ++number;

            this.collector.emit(new Values(number), number);

            System.out.println("Spout: " + number);

            Utils.sleep(1000);
        }

        @Override
        public void ack(Object msgId) {
//            super.ack(msgId);
            System.out.println("ack invoked ..." + msgId);
        }

        @Override
        public void fail(Object msgId) {
//            super.fail(msgId);
            System.out.println(" fail invoked ..." + msgId);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("num"));
        }
    }

    public static class SumBolt extends BaseRichBolt{

        private OutputCollector collector;
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        int sum = 0;
        @Override
        public void execute(Tuple input) {
            Integer value = input.getIntegerByField("num");
            sum += value;

            if(value > 0 && value <= 10){
                this.collector.ack(input);
            }else{
                this.collector.fail(input);
            }

            System.out.println("bolt: sum = [" + sum + "]");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SumBolt", new SumBolt()).shuffleGrouping("DataSourceSpout");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalSumStormAckerTopology", new Config(), builder.createTopology());

    }
}