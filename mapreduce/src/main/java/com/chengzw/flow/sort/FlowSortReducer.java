package com.chengzw.flow.sort;

import com.chengzw.flow.sum.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @description 拿到Mapper处理后的结果输出到文本中
 * @author chengzw
 * @since 2021/5/20 8:39 下午
 */
public class FlowSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    /**
     * @param key  flowBean对象
     * @param values 手机号
     * @param context 上下文，载体
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //手机号虽然是iterator，但是里面只有一个值，取一个即可
        context.write(values.iterator().next(),key);
    }
}
