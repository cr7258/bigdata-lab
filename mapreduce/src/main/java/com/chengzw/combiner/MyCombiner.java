package com.chengzw.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chengzw
 * @description 在Mapper阶段合并数据，例如 <hello,1,1> 合并为 </hello,2>，减轻Reduce阶段负担
 * @since 2021/5/20
 */
public class MyCombiner extends Reducer<Text,LongWritable,Text,LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;

        for (LongWritable value : values) {
            count += value.get();
        }

        context.write(key,new LongWritable(count));
    }
}
