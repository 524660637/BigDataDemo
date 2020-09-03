package com.bwda.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Dedup extends Configured implements Tool {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text MapOutputkey = new Text();
        private IntWritable MapOutputValue = new IntWritable(1);
        private static Text line=new Text();//每行数据
        private StringBuilder stringBuilder = new StringBuilder();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           if(value != null && value.getLength()>0){
               MapOutputkey.set(value);
               System.out.println(key+","+value+","+MapOutputValue);
               context.write(MapOutputkey, MapOutputValue);
           }
        }
    }
    public static class MyCombine extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable CombineOutputValue = new IntWritable(0);
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += Integer.parseInt(value.toString());
            }
            CombineOutputValue.set(sum);
            System.out.println(key+","+CombineOutputValue);
            context.write(key, CombineOutputValue);
        }
    }
    public static class MyPartition extends Partitioner<IntWritable, IntWritable> {
        @Override
        public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
            int Maxnumber = 65223;
            int bound = Maxnumber / numPartitions + 1;
            int keynumber = key.get();
            for (int i = 0; i < numPartitions; i++) {
                if (keynumber < bound * i && keynumber >= bound * (i - 1)) {
                    return i - 1;
                }
            }
            return 0;
        }
    }
    public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable countvalue = new IntWritable(1);
        @Override
        protected void reduce(Text inputkey, Iterable<IntWritable> inputvalue, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : inputvalue) {
                sum = sum + i.get();
            }
             System.out.println("key: "+inputkey + "...."+sum);
            countvalue.set(sum);
            context.write(inputkey, countvalue);
        }
    }
    public int run(String[] args) throws Exception {
        Job job  = Job.getInstance(this.getConf(), Dedup.class.getSimpleName());
        // set mainclass
        job.setJarByClass(Dedup.class);
        // set mapper
        job.setMapperClass(MyMapper.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(MyCombine.class);
        // set reducer
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // set path
        Path inpath = new Path(args[0]);
        FileInputFormat.setInputPaths(job, inpath);
        Path outpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outpath);
//        FileSystem fs = FileSystem.get(conf);
//        // 存在路径就删除
//        if (fs.exists(outpath)) {
//            fs.delete(outpath, true);
//        }
        job.setNumReduceTasks(1);
        boolean status = job.waitForCompletion(true);
        if (!status) {
            System.err.println("the job is error!!");
        }
        return status ? 0 : 1;
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //输入路径
        String localInput ="D:\\hadoop-2.6.0\\user\\wang\\DedupIn";
        //输出路径，必须是不存在的，空文件加也不行。
        String localOutput = "D:\\hadoop-2.6.0\\user\\wang\\DedupOut";
        System.setProperty("hadoop.home.dir","D:\\hadoop-2.6.0");
        Configuration conf = new Configuration();
        String[] arg = {localInput,localOutput};
        int atatus;
        try {
            atatus = ToolRunner.run(conf, new Dedup(),arg);
            System.exit(atatus);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

