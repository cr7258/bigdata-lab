package com.chengzw.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

/**
 * @description 排序
 * 一行有两个值
    a 1
    b 2
    a 3
    a 3
    c 2
    c 5
    b 6
 * 先按照第一个值排序，第一个值相同时，再按照第二个值排序
 * @author chengzw
 * @since 2021/5/20 8:39 下午
 */
public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"sort");

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/tmp/input/sort.txt"));

        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(MySortBean.class);
        job.setMapOutputValueClass(NullWritable.class);


        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(MySortBean.class);
        job.setMapOutputValueClass(NullWritable.class);


        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("/tmp/output/sort"));

        boolean b = job.waitForCompletion(true);
        System.out.println(b==true?"MapReduce 任务执行成功!":"MapReduce 任务执行失败!");
        System.exit(b ? 0 : 1);

    }
}
