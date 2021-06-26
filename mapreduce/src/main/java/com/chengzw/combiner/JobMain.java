package com.chengzw.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @description 在Mapper阶段合并数据，例如 <hello,1,1> 合并为 </hello,2>，减轻Reduce阶段负担
 * @author chengzw
 * @since 2021/5/20 8:39 下午
 */
public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"combiner");

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/tmp/input/mr.txt"));

        job.setMapperClass(WordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setCombinerClass(MyCombiner.class);

        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);


        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("/tmp/output/combiner"));

        boolean b = job.waitForCompletion(true);
        System.out.println(b==true?"MapReduce 任务执行成功!":"MapReduce 任务执行失败!");
        System.exit(b ? 0 : 1);

    }
}
