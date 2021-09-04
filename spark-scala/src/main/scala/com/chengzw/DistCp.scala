package com.chengzw

import com.chengzw.InvertIndex.options
import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * @description
 * @author chengzw
 * @since 2021/9/4
 */
object DistCp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName(this.getClass().getName())
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //val input = "/Users/chengzhiwei/Code/github/bigdata-lab/spark-scala/src/main/resources/distcp/input"
    //val output = "/Users/chengzhiwei/Code/github/bigdata-lab/spark-scala/src/main/resources/distcp/output"

    //解析输入参数
    val options = new Options()
    options.addOption("i", "ignore failure", false, "ignore failure")
    options.addOption("m", " max concurrence", true, "max concurrence")
    options.addOption("input", "input file", true, "input file")
    options.addOption("output", "output file", true, "output file")
    val parser = new DefaultParser()
    val cmd = parser.parse(options, args)

    val IGNORE_FAILURE = cmd.hasOption("i")
    val MAX_CONNCURRENCES = if (cmd.hasOption("m")) cmd.getOptionValue("m").toInt
    else 2
    val input = cmd.getOptionValue("input")
    val output = cmd.getOptionValue("output")
    println("输入参数 ignore failure: " + IGNORE_FAILURE + " , max concurrence: " + MAX_CONNCURRENCES + " , input file: " + input + " , output file: " + output)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val fileList = fs.listFiles(new Path(input), true)

    val arrayBuffer = ArrayBuffer[String]()
    while (fileList.hasNext) {
      val path = fileList.next.getPath.toString
      arrayBuffer.append(path)
      println(path)
    }

    val rdd = sc.parallelize(arrayBuffer, MAX_CONNCURRENCES)
    rdd.foreachPartition(it => {
      val conf = new Configuration()
      val sfs = FileSystem.get(conf)

      while (it.hasNext) {
        val src = it.next
        val tgt = src.replace(input, output)
        try {
          FileUtil.copy(sfs, new Path(src), sfs, new Path(tgt), false, conf)
        } catch {
          case ex: Exception =>
            if (IGNORE_FAILURE) println("ignore failure when copy")
            else throw ex
        }
      }
    })
  }
}
