package com.chengzw


import org.apache.commons.cli.{Options, PosixParser}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @description 使用 RDD API 实现带词频的倒排索引
 * @author chengzw
 * @since 2021/9/4
 */
object InvertIndex extends App {

  val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass().getName())
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("WARN")

  //val input = "/Users/chengzhiwei/Code/github/bigdata-lab/spark-scala/src/main/resources/invertIndex"
  //解析输入参数
  val options = new Options()
  options.addOption("f", "input file", true, "input file")
  val parser = new PosixParser()
  val cmd = parser.parse(options, args)
  val input = cmd.getOptionValue("f")
  println("输入目录名: " + input)

  val fs = FileSystem.get(sc.hadoopConfiguration)
  val fileList = fs.listFiles(new Path(input),true)
  var rdd = sc.emptyRDD[(String,String)] //filename,word
  while(fileList.hasNext){
    val path = fileList.next
    val fileName = path.getPath.getName
          //合并3个文件的结果
    rdd = rdd.union(sc.textFile(path.getPath.toString)
      //将文件中的单词按空格拆分
      .flatMap(_.split("\\s+"))
      //映射文件名和单词
      .map((fileName,_)))
  }

  println("--"*10)
  //输出(filename,word)
  rdd.foreach(println)
  println("--"*10)

  //文件内统计词频  word => (word,1)    (file_0,it),2
  val rdd2 = rdd.map((_,1)).reduceByKey(_+_)
  println("--"*10)
  rdd2.foreach(println)
  println("--"*10)

  //单词对应文件名，倒排索引
  val rdd3 = rdd2.map(data => (data._1._2,String.format("[%s,%s]",data._1._1,data._2.toString)))
    //根据单词进行分组 "is",{[file_0,2],[file_2,1],[file_1,1]}
    .reduceByKey(_ + "," + _)
    .map(data => String.format("\"%s\",{%s}",data._1,data._2))
  println("--"*10)
  rdd3.foreach(println)
  println("--"*10)

}
