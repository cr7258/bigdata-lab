package com.chengzw.flow.sum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * @description 将相同手机号的flowBean的相同属性值累加
 * @author chengzw
 *  * @since 2021/5/20 8:39 下午
 * Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 *     KEYIN：手机号的类型
 *     VALUEIN： FlowBean
 *     KEYOUT：Text类型
 *     VALUEOUT：FlowBean
 */

public class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean> {

    /**
     *
     * @param key 手机号
     * @param values  相同号的flowBean对象集合
     * @param context 上下文，载体
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        //1、遍历values，将四个字段的值进行累加
        Integer upFlow = 0;
        Integer downFlow = 0;
        Integer upCountFlow = 0;
        Integer downCountFlow = 0;
        for (FlowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }

        //2、创建FlowBean对象，给这个对象赋值
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(upFlow);
        flowBean.setDownFlow(downFlow);
        flowBean.setUpCountFlow(upCountFlow);
        flowBean.setDownCountFlow(downCountFlow);

        //3、写入到上下文
        context.write(key,flowBean);
    }
}
