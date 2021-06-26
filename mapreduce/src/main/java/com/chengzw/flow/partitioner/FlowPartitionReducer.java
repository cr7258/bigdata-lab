package com.chengzw.flow.partitioner;

import com.chengzw.flow.sum.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @description 拿到Mapper处理后的结果输出到文本中
 * @author chengzw
 * @since 2021/5/20 8:39 下午
 */
public class FlowPartitionReducer extends Reducer<Text, FlowBean,Text,FlowBean> {

    /**
     * @param key  手机号
     * @param values flowBean对象
     * @param context 上下文，载体
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        context.write(key,values.iterator().next());
    }
}
