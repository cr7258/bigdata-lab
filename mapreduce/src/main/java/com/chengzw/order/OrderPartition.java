package com.chengzw.order;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author chengzw
 * @description 根据订单号分区，输出到不同的文件
 * 分区的目的是根据Key值决定Mapper的输出记录被送到哪一个Reducer上去处理。
 * @since 2021/6/11
 */
public class OrderPartition extends Partitioner<OrderBean, Text> {

    /**
     *
     * @param orderBean k2
     * @param text v2
     * @param numPartitions ReduceTask的个数
     * @return  返回的是分区的编号：比如说：ReduceTask的个数3个，返回的编号是 0 1 2
     *                              比如说：ReduceTask的个数2个，返回的编号是 0 1
     *                              比如说：ReduceTask的个数1个，返回的编号是 0
     */
    @Override
    public int getPartition(OrderBean orderBean, Text text, int numPartitions) {
        //参考源码  return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        //按照key的hash值进行分区，解决int类型数据hashcode值取模出现负数而影响分区
        //key.hashCode() & Integer.MAX_VALUE 是要保证任何map输出的key在numReduceTasks取模后决定的分区为正整数。
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
