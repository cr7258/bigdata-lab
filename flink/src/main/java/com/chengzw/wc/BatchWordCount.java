package com.chengzw.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author chengzw
 * @description 批处理 wordcount
 * @since 2022/1/10
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 创建批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据
        String inputPath = "D:\\Code\\bigdata-lab\\flink\\src\\main\\resources\\hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        // 对数据集进行处理
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper()) // 按空格分词展开，转换成 (word, 1) 二元组进行统计
                .groupBy(0)  // 按照第一个位置的 word 分组
                .sum(1);   // 按照第二个位置上的数据求和

        resultSet.print();
    }

    // 自定义类，实现 FlatMapFunction 接口
    // Tuple2 是 Flink 实现的二元组
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 按空格分词
            String[] words = value.split(" ");
            // 遍历所有word，包成二元组输出
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
