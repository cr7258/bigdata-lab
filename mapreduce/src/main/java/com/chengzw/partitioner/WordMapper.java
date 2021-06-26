package com.chengzw.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

/**
 * @author chengzw
 * @since 2021/5/17 10:00 下午
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、切分单词
        String[] words = value.toString().split(" ");

        //2、单词转换 单词 -> <单词,1>
        for (String word : words) {
            //3、写入到上下文
            context.write(new Text(word),new LongWritable(1));
        }
    }
}
