package com.bwda.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Temperature {
    /**
     * 四个泛型类型分别代表：
     * KeyIn        Mapper的输入数据的Key，这里是每行文字的起始位置（0,11,...）
     * ValueIn      Mapper的输入数据的Value，这里是每行文字
     * KeyOut       Mapper的输出数据的Key，这里是每行文字中的“年份”
     * ValueOut     Mapper的输出数据的Value，这里是每行文字中的“气温”
     */
    static class TempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 打印样本: Before Mapper: 0, 2000010115
            if(value != null && value.getLength()>0){
                System.out.print("Before Mapper: " + key + ", " + value);
                String line = value.toString();
                String year = line.substring(0,4);
                int temperature = Integer.parseInt(line.substring(8));
                context.write(new Text(year), new IntWritable(temperature));
                // 打印样本: After Mapper:2000, 15
                System.out.println(
                        "======"+
                                "After Mapper:" + new Text(year) + ", " + new IntWritable(temperature));
            }
            }

    }

    /**
     * 四个泛型类型分别代表：
     * KeyIn        Reducer的输入数据的Key，这里是每行文字中的“年份”
     * ValueIn      Reducer的输入数据的Value，这里是每行文字中的“气温”
     * KeyOut       Reducer的输出数据的Key，这里是不重复的“年份”
     * ValueOut     Reducer的输出数据的Value，这里是这一年中的“最高气温”
     */
    static class TempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int maxValue = Integer.MIN_VALUE;
            StringBuffer sb = new StringBuffer();
            //取values的最大值
            for(IntWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
                sb.append(value).append(", ");
            }
            // 打印样本： Before Reduce: 2000, 15, 23, 99, 12, 22,
            System.out.print("Before Reduce: " + key + ", " + sb.toString());
            context.write(key,new IntWritable(maxValue));
            // 打印样本： After Reduce: 2000, 99
            System.out.println(
                    "======"+
                            "After Reduce: " + key + ", " + maxValue);
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException  {

        //输入路径
        String input = "/user/wang/input";
        String localInput ="D:\\hadoop-2.6.0\\user\\wang\\input";
        //输出路径，必须是不存在的，空文件加也不行。
        String output = "/user/wang/output";
        String localOutput = "D:\\hadoop-2.6.0\\user\\wang\\output";
        System.setProperty("hadoop.home.dir","D:\\hadoop-2.6.0");
//        System.setProperty("HADOOP_USER_NAME", "hdfs");
//        System.setProperty("hdp.version","2.3.0.0-2557");
        Configuration hadoopConfig = new Configuration();
//        hadoopConfig.set("fs.defaultFS","hdfs://192.168.2.38:8020");
//        hadoopConfig.set("fs.defaultFS", "file:///");
//        hadoopConfig.set("mapreduce.framework.name", "local");
//        hadoopConfig.set("mapreduce.framework.name", "yarn");
//        hadoopConfig.set("yarn.resourcemanager.hostname", "hdp.master");
//        hadoopConfig.set("yarn.resourcemanager.address","hdp.master:8050");
//        hadoopConfig.set("yarn.resourcemanager.admin.address","hdp.master:8141");
//        hadoopConfig.set("yarn.resourcemanager.scheduler.address","hdp.master:8030");
//        hadoopConfig.set("yarn.resourcemanager.resource-tracker.address","hdp.master:8025");
//        hadoopConfig.set("mapreduce.job.jar", "D:\\mr\\target\\mr-1.0-SNAPSHOT.jar");
//        hadoopConfig.set("mapreduce.app-submission.cross-platform","true");
//        hadoopConfig.set("yarn.application.classpath","/usr/hdp/2.3.0.0-2557/hadoop/conf:/usr/hdp/2.3.0.0-2557/hadoop/lib/*:/usr/hdp/2.3.0.0-2557/hadoop/.//*:/usr/hdp/2.3.0.0-2557/hadoop-hdfs/./:/usr/hdp/2.3.0.0-2557/hadoop-hdfs/lib/*:/usr/hdp/2.3.0.0-2557/hadoop-hdfs/.//*:/usr/hdp/2.3.0.0-2557/hadoop-yarn/lib/*:/usr/hdp/2.3.0.0-2557/hadoop-yarn/.//*:/usr/hdp/2.3.0.0-2557/hadoop-mapreduce/lib/*:/usr/hdp/2.3.0.0-2557/hadoop-mapreduce/.//*:::/usr/share/java/mysql-connector-java.jar:/usr/hdp/2.3.0.0-2557/tez/*:/usr/hdp/2.3.0.0-2557/tez/lib/*:/usr/hdp/2.3.0.0-2557/tez/conf");

        Job job  = Job.getInstance(hadoopConfig,TempMapper.class.getSimpleName());
        //如果需要打成jar运行，需要下面这句
//        job.setJarByClass(Temperature.class);

        //job执行作业时输入和输出文件的路径
//        FileInputFormat.addInputPath(job,new Path(input));
//        FileOutputFormat.setOutputPath(job,new Path(output));
        FileInputFormat.addInputPath(job,new Path(localInput));
        FileOutputFormat.setOutputPath(job,new Path(localOutput));
        //指定自定义的Mapper和Reducer作为两个阶段的任务处理类
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);

        //设置最后输出结果的Key和Value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //执行job，直到完成
        job.waitForCompletion(true);
        System.err.println("Finished");
    }

}
