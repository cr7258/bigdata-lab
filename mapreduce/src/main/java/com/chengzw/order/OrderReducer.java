package com.chengzw.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chengzw
 * @description Reduce
 * @since 2021/6/11
 */
public class OrderReducer extends Reducer<OrderBean, Text,Text, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        //OrderBean中实现了compareTo方法，这里的values是已经排好序的了，先比较订单的id，如果id一样，则将订单的商品按金额排序（降序）
        //获取top N ,下面的代码就是取出来top3。
        for (Text value : values) {
            context.write(value,NullWritable.get());
            i++;
            if (i >= 3){
                break;
            }
        }
    }
}
