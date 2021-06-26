package com.chengzw.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author chengzw
 * @since 2021/5/20
 */
public class SortMapper extends Mapper<LongWritable, Text, MySortBean, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");

        MySortBean mySortBean = new MySortBean();
        mySortBean.setWord(split[0]);
        mySortBean.setNum(Integer.parseInt(split[1]));

        //key是sortBean对象，值是null
        context.write(mySortBean,NullWritable.get());

    }
}
