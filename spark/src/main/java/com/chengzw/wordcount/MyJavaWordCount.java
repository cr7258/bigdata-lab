package com.chengzw.wordcount;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @description WordCount 示例
 * @author chengzw
 * @since 2021/7/25 8:39 下午
 */
public class MyJavaWordCount {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        System.setProperty("spark.ui.showConsoleProgress","false");
        //创建配置对象
        //本地运行
        //SparkConf conf = new SparkConf().setAppName("MyJavaWordCount").setMaster("local");
        //在Spark上运行
        SparkConf conf = new SparkConf().setAppName("MyJavaWordCount");
        //创建SparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        //读取hdfs数据
        //在本地运行
        //JavaRDD<String> rdd1= sc.textFile("/tmp/data.txt");
        //在Spark上运行
        JavaRDD<String> rdd1= sc.textFile(args[0]);

        //分词
        JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String input) throws Exception {
                return Arrays.asList(input.split(" ")).iterator();
            }
        });
        //单词计数 word,1
        JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        //相同Key的值累加
        JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });

        //触发计算
        List<Tuple2<String, Integer>> result = rdd4.collect();

        //打印
        for(Tuple2<String,Integer> r : result){
            System.out.println(r._1 + "\t" + r._2);
        }

        //释放资源
        sc.stop();

    }
}