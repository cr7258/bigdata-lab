package com.chengzw.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author chengzw
 * @description 求订单最大值的主类
 * @since 2021/6/11
 */
public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //一、初始化一个job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "mygroup");

        //二、配置Job信息
        //1.设置输入信息
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/tmp/input/orders.txt"));

        //2.设置mapper 并设置输出的键和输出的值
        job.setMapperClass(OrderMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(Text.class);

        //3.分区设置，根据订单编号orderId分区，输出到不同的文件
        job.setPartitionerClass(OrderPartition.class);

        //4.分组设置，根据订单编号orderId分区，因为同一个文件中可能有多个订单号
        job.setGroupingComparatorClass(OrderGroup.class);

        //5.设置Reducer,并设置输出的键和输出的值，计算出每个订单号中金额最大的3个商品
        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //设置NumReduceTask（分区）的个数  默认是1
        job.setNumReduceTasks(3);

        //6.设置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("/tmp/output/order"));

        //三、等待完成  实际上就是提交任务
        boolean b = job.waitForCompletion(true);
        System.out.println(b==true?"MapReduce 任务执行成功!":"MapReduce 任务执行失败!");
        System.exit(b ? 0 : 1);
    }
}
