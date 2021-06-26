package com.chengzw.flow.partitioner;

import com.chengzw.flow.sum.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @description 读取文件内容，将每行内容序列化为flowBean对象，key是手机号，value是flowBean对象，写入到上下文中
 * @author chengzw
 * @since 2021/5/20 8:39 下午
 */
public class FlowPartitionMapper extends Mapper<LongWritable, Text,Text, FlowBean> {

    /**
     * @param key 偏移量的类型
     * @param value 一行文本数据类型
     * @param context 上下文，载体
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String phoneNum = split[0];

        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[1]));
        flowBean.setDownFlow(Integer.parseInt(split[2]));
        flowBean.setUpCountFlow(Integer.parseInt(split[3]));
        flowBean.setDownCountFlow(Integer.parseInt(split[4]));

        context.write(new Text(phoneNum),flowBean);
    }
}
