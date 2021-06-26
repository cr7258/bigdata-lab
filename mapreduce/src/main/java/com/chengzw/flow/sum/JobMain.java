package com.chengzw.flow.sum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * 需求一：统计每个手机号的数据包和流量总和
 * @description 主类：将Mapper和Reducer阶段串联起来，提供了程序运行的入口
 * @author chengzw
 * @since 2021/5/20 8:39 下午
 */
public class JobMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //一、初始化一个Job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "flowsum");

        //二、设置Job对象的各种信息，里面包含了8个小步骤
        //1、设置输入路径，让程序找到源文件的位置
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/tmp/flow/flow.log"));

        //2、设置Mapper类型，并设置k2 v2
        job.setMapperClass(FlowCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //Shuffle阶段，现在使用默认的就可以

        //7、设置Reducer类型，并设置k3 v3
        job.setReducerClass(FlowCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //8、设置输出路径，让结果文件存储到某个地方去
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("/tmp/flow/sum"));

        //三、等待程序完成，提交
        boolean b = job.waitForCompletion(true);
        System.out.println(b==true?"MapReduce 任务执行成功!":"MapReduce 任务执行失败!");
        System.exit(b ? 0 : 1);
    }
}
