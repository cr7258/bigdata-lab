package com.chengzw.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


/**
 * @description Driver驱动类
 * @author chengzw
 * @since 2021/5/20 8:39 下午
 */

public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //一、初始化Job
        Configuration configuration = new Configuration();

        //获取运行命令的参数，参数一：输入文件路径，参数二：输出文件路径
        //如果输入路径是一个文件，那么只处理这个文件，如果指定的路径是目录，则处理这个目录下的所有文件
        //输出路径只能是不存在的目录名
        String [] otherArgs = new GenericOptionsParser(configuration,args).getRemainingArgs();
        if(otherArgs.length < 2){
            System.err.println("必须提供输入文件路径和输出文件路径");
            System.exit(2);
        }
        Job job = Job.getInstance(configuration, "mr");

        //二、设置Job的相关信息 8个小步骤
        //1、设置输入路径
        job.setInputFormatClass(TextInputFormat.class);
        //本地运行
        //TextInputFormat.addInputPath(job,new Path("/tmp/input/mr.txt"));
        TextInputFormat.addInputPath(job,new Path(args[0]));

        //2、设置Mapper类型，并设置输出的键和输出的值
        job.setMapperClass(WordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //shuffle阶段，使用默认的

        //3、设置Reducer类型，并设置输出的键和输出的值
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //4、设置输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        //本地运行
        //TextOutputFormat.setOutputPath(job,new Path("/tmp/output/mr"));
        TextOutputFormat.setOutputPath(job,new Path(args[1]));


        //三、等待完成
        boolean b = job.waitForCompletion(true);
        System.out.println(b==true?"MapReduce 任务执行成功!":"MapReduce 任务执行失败!");
        System.exit(b ? 0 : 1);
    }
}