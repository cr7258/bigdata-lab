

# 使用RDD API实现带词频的倒排索引

使用RDD API实现带词频的倒排索引。倒排索引（Inverted index），也被称为反向索引。它是文档检索系统中最常用的数据结构。被广泛地应用于全文搜索引擎。

例子如下，被索引的文件为（file_0，file_1，file_2代表文件名） 

file_0 "it is what it is"

file_1. "what is it"

file_2 "it is a banana"

我们就能得到下面的反向文件索引：

"a": {2}

"banana": {2}

"is": {0, 1, 2}

"it": {0, 1, 2}

"what": {0, 1}

再加上词频为：

"a": {(2,1)}

"banana": {(2,1)}

"is": {(0,2), (1,1), (2,1)}

"it": {(0,2), (1,1), (2,1)}

"what": {(0,1), (1,1)}



本地测试：

![](https://chengzw258.oss-cn-beijing.aliyuncs.com/Article/20210904173349.png)



本地测试完毕后，将编写好的 scala 代码打包成 jar 包。使用 assembly 插件将依赖打成一个 jar 包。



执行以下命令打包：

```sh
mvn clean compile assembly:single
```

![](https://chengzw258.oss-cn-beijing.aliyuncs.com/Article/20210904185417.png)



通过 spark-submit 运行代码：

```sh
bin/spark-submit --class com.chengzw.InvertIndex \
/Users/chengzhiwei/Code/github/bigdata-lab/spark-scala/target/spark-scala-1.0-SNAPSHOT-jar-with-dependencies.jar -f /Users/chengzhiwei/Code/github/bigdata-lab/spark-scala/src/main/resources/invertIndex

#返回结果
输入目录名: /Users/chengzhiwei/Code/github/bigdata-lab/spark-scala/src/main/resources/invertIndex
--------------------
#先将文件中的单词按照空格拆分
(file_0,it)
(file_0,is)
(file_0,what)
(file_0,it)
(file_0,is)
(file_1,what)
(file_1,is)
(file_1,it)
(file_2,it)
(file_2,is)
(file_2,a)
(file_2,banana)
--------------------
#文件内统计词频
((file_0,it),2)
((file_2,it),1)
((file_1,it),1)
((file_2,banana),1)
((file_0,is),2)
((file_2,a),1)
((file_2,is),1)
((file_1,is),1)
((file_1,what),1)
((file_0,what),1)
--------------------
#根据单词进行分组，得到最终结果
"is",{[file_0,2],[file_2,1],[file_1,1]}
"a",{[file_2,1]}
"what",{[file_1,1],[file_0,1]}
"banana",{[file_2,1]}
"it",{[file_0,2],[file_2,1],[file_1,1]}
--------------------
```



## Distcp 的 Spark 实现

使用 Spark 实现 Hadoop 分布式数据传输工具 DistCp (distributed copy)，只要求实现最基础的 copy 功

能，对于-update、-diff、-p不做要求。

对于 HadoopDistCp 的功能与实现，可以参考：

https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html

https://github.com/apache/hadoop/tree/release-2.7.1/hadoop-tools/hadoop-distcp

Hadoop 使用 MapReduce 框架来实现分布式 copy，在 Spark 中应使用 RDD 来实现分布式 copy 

应实现的功能为：

sparkDistCp hdfs://xxx/source hdfs://xxx/target

得到的结果为，启动多个 task/executor，将 hdfs://xxx/source 目录复制到 hdfs://xxx/target，得到

hdfs://xxx/target/source。

需要支持 source 下存在多级子目录。

需支持 -i Ignore failures 参数。

需支持 -m max concurrence 参数，控制同时 copy 的最大并发 task 数。

-input 输入的目录，-ouput 输出的目录。



通过 spark-submit 运行代码：

```sh
bin/spark-submit --class com.chengzw.DistCp \
/Users/chengzhiwei/Code/github/bigdata-lab/spark-scala/target/spark-scala-1.0-SNAPSHOT-jar-with-dependencies.jar -i -m 4 \
-input /Users/chengzhiwei/Code/github/bigdata-lab/spark-scala/src/main/resources/distcp/input \
-output /Users/chengzhiwei/Code/github/bigdata-lab/spark-scala/src/main/resources/distcp/output

#返回结果
输入参数 ignore failure: true , max concurrence: 4 , input file: /Users/chengzhiwei/Code/github/bigdata-lab/spark-scala/src/main/resources/distcp/input , output file: /Users/chengzhiwei/Code/github/bigdata-lab/spark-scala/src/main/resources/distcp/output
file:/Users/chengzhiwei/Code/github/bigdata-lab/spark-scala/src/main/resources/distcp/input/dict1/file2
file:/Users/chengzhiwei/Code/github/bigdata-lab/spark-scala/src/main/resources/distcp/input/file1
```

![](https://chengzw258.oss-cn-beijing.aliyuncs.com/Article/20210904173624.png)
