package com.chengzw.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chengzw
 * @since 2021/5/20
 */
public class SortReducer extends Reducer<MySortBean, NullWritable, MySortBean, NullWritable> {

    @Override
    protected void reduce(MySortBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        context.write(key,NullWritable.get());
    }
}
