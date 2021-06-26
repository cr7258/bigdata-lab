package com.chengzw.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chengzw
 * @description Reduce 阶段，把 key 相同的数据进行累计，得到每个单词出现的次数
 * @since 2021/5/17 10:00 下午
 */
public class WordReducer extends Reducer<Text, LongWritable,Text,LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //1、定义一个变量
        long count = 0;

        //2、迭代
        for (LongWritable value : values) {
            count += value.get();
        }

        //3、写入上下文
        context.write(key,new LongWritable(count));
    }
}
