package com.chengzw.partitioner;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


/**
 * @description 使用分区策略将结果输出到不同的文件
 * @author chengzw
 * @since 2021/5/20 8:39 下午
 */

public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //一、初始化Job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "partitioner");

        //二、设置Job的相关信息 8个小步骤
        //1、设置输入路径
        job.setInputFormatClass(TextInputFormat.class);
        //本地运行
        TextInputFormat.addInputPath(job,new Path("/tmp/input/mr.txt"));
        //在hdfs上运行，注意运行程序的用户需要在hdfs有文件的读写权限
        //TextInputFormat.addInputPath(job,new Path("hdfs://hadoop1:8020/tmp/input/mr.txt"));

        //2、设置Mapper类型，并设置k2 v2
        job.setMapperClass(WordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //shuffle阶段
        //3、设置分区
        job.setPartitionerClass(MyPartitioner.class);
        //设置NumReduceTask的个数，也是分区数量，结果要分成2个文件存放
        job.setNumReduceTasks(2);

        //7、设置Reducer类型，并设置k3 v3
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //8、设置输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        //本地运行
        TextOutputFormat.setOutputPath(job,new Path("/tmp/output/partitioner"));
        //在hdfs上运行
        //TextOutputFormat.setOutputPath(job,new Path("hdfs://hadoop1:8020/tmp/output/partitioner"));

        //三、等待完成
        boolean b = job.waitForCompletion(true);
        System.out.println(b==true?"MapReduce 任务执行成功!":"MapReduce 任务执行失败!");
        System.exit(b ? 0 : 1);
    }
}