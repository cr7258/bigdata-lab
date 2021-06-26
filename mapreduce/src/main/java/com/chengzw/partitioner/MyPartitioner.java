package com.chengzw.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @description 自定义分区策略，根据单词的长度进行区分，长度 >= 5的在一个文件中，长度 < 5的在另一个文件中
 * @author chengzw
 * @since 2021/5/20 8:33 下午
 * Partitioner<KEY,VALUE>
 *     KEY 单词的类型
 *     VALUE 单词出现的次数
 */
public class MyPartitioner extends Partitioner<Text, LongWritable> {

    @Override
    public int getPartition(Text text, LongWritable longWritable, int numPartitions) { //不设置为1

        // 根据单词的长度进行区分，长度 >= 5的在一个文件中，长度 < 5的在另一个文件中
        if(text.toString().length() >=5){
            return 0;
        }else {
            return 1;
        }
    }
}
