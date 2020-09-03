package com.bwda.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Sort extends Configured implements Tool {

    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private IntWritable MapOutputkey = new IntWritable() ;
        private IntWritable MapOutputValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(value != null && value.getLength()>0){
                MapOutputkey.set(Integer.parseInt(value.toString()));
                System.out.println(MapOutputkey.toString()+","+MapOutputValue.toString());
                context.write(MapOutputkey, MapOutputValue);
            }
        }
    }
    public static class MyCombine extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private Integer rank = 0;
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                System.out.println(key.toString()+","+value.toString());
                rank++;
                System.err.println(rank.toString()+","+key.toString());
                context.write(new IntWritable(rank),key);
            }

        }
    }

    public static class MyReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private Integer rank = 0;
        @Override
        protected void reduce(IntWritable inputkey, Iterable<IntWritable> inputvalue, Context context) throws IOException, InterruptedException {
            for (IntWritable i : inputvalue) {
                rank++;
                System.err.println("rank: "+rank.toString()+","+inputkey );
                context.write(new IntWritable(rank), inputkey);
            }

        }
    }
    public int run(String[] args) throws Exception {
        Job job  = Job.getInstance(this.getConf(), Sort.class.getSimpleName());
        // set mainclass
        job.setJarByClass(Sort.class);
        // set mapper
        job.setMapperClass(Sort.MyMapper.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);

//        job.setCombinerClass(MyCombine.class);
        // set reducer
        job.setReducerClass(Sort.MyReduce.class);
        job.setOutputKeyClass(IntWritable.class);
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
        String localInput ="D:\\hadoop-2.6.0\\user\\wang\\sortIn";
        //输出路径，必须是不存在的，空文件加也不行。
        String localOutput = "D:\\hadoop-2.6.0\\user\\wang\\sortOut";
        System.setProperty("hadoop.home.dir","D:\\hadoop-2.6.0");
        Configuration conf = new Configuration();
        String[] arg = {localInput,localOutput};
        int atatus;
        try {
            atatus = ToolRunner.run(conf, new Sort(),arg);
            System.exit(atatus);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
