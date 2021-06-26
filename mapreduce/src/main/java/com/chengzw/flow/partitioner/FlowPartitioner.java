package com.chengzw.flow.partitioner;

import com.chengzw.flow.sum.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @description 自定义的分区策略
 * 手机号码分区
 *  135 开头放一个文件
 *  136 开头放一个文件
 *  137 开头放一个文件
 *  其他开头  放一个文件
 * @author chengzw
 * @since 2021/5/23
 */
public class FlowPartitioner extends Partitioner<Text, FlowBean> {

    /**
     *
     * @param text 手机号
     * @param flowBean flowBean对象
     * @param numPartitions 在JobMain类中setNumReduceTasks指定的分区数量
     * @return
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        System.out.println(numPartitions);
        String phoneNumberPrefix = text.toString().substring(0,3);
        switch (phoneNumberPrefix){
            case "135":
                return 0;
            case "136":
                return 1;
            case "137":
                return 2;
            default:
                return 3;
        }
    }
}
