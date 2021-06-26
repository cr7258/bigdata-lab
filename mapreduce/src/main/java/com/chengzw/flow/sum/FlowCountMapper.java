package com.chengzw.flow.sum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @description 读取文件内容，将每行内容序列化为flowBean对象，key是手机号，value是flowbean对象，写入到上下文中
 * @author chengzw
 * @since 2021/5/20 8:39 下午
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     KEYIN 偏移量的类型
 *     VALUEIN 一行文本数据的类型
 *     KEYOUT 手机号的类型
 *     VALUEOUT   FlowBean
 *
 * flow.log 数据格式
1363157995033  15920133257	5C-0E-8B-C7-BA-20:CMCC	120.197.40.4	sug.so.360.cn	信息安全	         20	      20	    3156	 2936	  200
     时间戳           手机号      基站编号                IP              URL           URL类型     上行数据包  下行数据包  上行流量 下行流量   响应码

 */

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    /**
     * @param key 偏移量的类型
     * @param value  一行文本数据类型
     * @param context   上下文，载体
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、拆分文本数据集，得到手机号和想要的数据
        String[] split = value.toString().split("\t");
        String phoneNum = split[1];

        //2、创建FlowBean对象，给要使用到的数据封装进去
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[6]));
        flowBean.setDownFlow(Integer.parseInt(split[7]));
        flowBean.setUpCountFlow(Integer.parseInt(split[8]));
        flowBean.setDownCountFlow(Integer.parseInt(split[9]));

        //3、写入到上下文中
        context.write(new Text(phoneNum),flowBean);
    }
}
