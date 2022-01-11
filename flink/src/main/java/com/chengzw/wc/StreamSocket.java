package com.chengzw.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chengzw
 * @description 流式数据源
 * @since 2022/1/11
 */
public class StreamSocket {
    public static void main(String[] args) throws Exception {
        // 创建批处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从 socket 文本流读取数据
        DataStream<String> inputDataStream = env.socketTextStream("127.0.0.1", 7777);

        // 基于数据流进行转换计算
        DataStream<Tuple2<String,Integer>> resultStream = inputDataStream.flatMap(new BatchWordCount.MyFlatMapper())
                .keyBy(0)  // 按照 key 分区
                .sum(1);

        // 设置并行度，默认值 = 当前计算机的 CPU 逻辑核数（设置成 1 即单线程处理）
        env.setMaxParallelism(8);

        // 输出结果
        resultStream.print();
        // 执行任务
        env.execute();
    }
}
