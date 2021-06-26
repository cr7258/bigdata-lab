# MapReduce 原理介绍与开发实战

MapReduce 是一个分布式运算程序的编程框架，核心功能是将用户编写的业务逻辑代码和自带默认组件整合成一个完整的分布式运算程序，并发地运行在 Hadoop 集群上。

## 为什么需要 MapReduce

海量数据在单机上处理受到硬件资源限制，而一旦将单机程序扩展到集群来分布式运行，将极大增加程序的复杂度和开发难度。为了提高开发效率，MapReduce 将分布式程序中的公共功能封装成框架。引入 MapReduce 框架后，开发人员可以将绝大部分工作集中在业务逻辑的开发上，而将分布式计算中复杂的工作交由框架来处理。

## MapReduce 处理阶段

MapReduce 框架通常由三个阶段组成：
* **Map**：读取文件数据，按照规则对文本进行拆分，生成 KV 形式的数据。
* **Shuffle**：工作节点根据输出键（由 map 函数生成）重新分配数据，对数据排序、分组、拷贝，目的是属于一个键的所有数据都位于同一个工作节点上。
* **Reduce**：工作节点并行处理每个键的一组数据，对结果进行汇总。

![](https://chengzw258.oss-cn-beijing.aliyuncs.com/Article/20210619233904.png)

下图把 MapReduce 的过程分为两个部分，而实际上从两边的 Map 和 Reduce 到中间的那一大块都属于 Shuffle 过程，也就是说，Shuffle 过程有一部分是在 Map 端，有一部分是在 Reduce 端。

![](https://chengzw258.oss-cn-beijing.aliyuncs.com/Article/20210625233654.png)

## Hadoop 序列化

### 为什么要序列化？

一般来说，“活的”对象只生存在内存里，关机断电就没有了。而且“活的”对象只能由本地的进程使用，不能被发送到网络上的另外一台计算机。然而序列化可以存储“活的”对象，将“活的”对象发送到远程计算机。序列化和反序列化在分布式数据处理领域经常出现，主要有两个作用：**进程通信和永久存储**。

### 什么是序列化？

序列化就是把内存中的对象，转换成字节序列（或其他数据传输协议）以便于存储（持久化）和网络传输。　　
反序列化就是将收到字节序列（或其他数据传输协议）或者是硬盘持久化的数据，转换成内存中的对象。

### 为什么不用 Java 的序列化？

Java 的序列化使用了一个重量级序列化框架（Serializable），一个对象被序列化后，会附带很多额外的信息（校验信息，header，继承关系等），不便于在网络中高效传输。所以，Hadoop 自己开发了一套更加精简和高效序列化机制（Writable）。

Hadoop 中各个节点的通信是通过远程调用（RPC）实现的，RPC 序列化要求具有以下特点：
* 紧凑：紧凑的格式能够充分利用网络带宽，减少在网络中的传输时间。
* 快速：需要尽量减少序列化和反序列化的性能开销。
* 互操作：能支持不同语言写的客户端和服务端进行交互；
* 支持常用数据的序列化。

### Java 常用的数据类型对应的 Hadoop 数据序列化类型

| Java类型  | Hadoop Writable类型 |
|---------|-------------------|
| Boolean | BooleanWritable   |
| Byte    | ByteWritable      |
| Int     | IntWritable       |
| Float   | FloatWritable     |
| Long    | LongWritable      |
| Double  | DoubleWritable    |
| String  | Text              |
| Map     | MapWritable       |
| Array   | ArrayWritable     |
| Null   | NullWritable     |


## MapReduce 进程

MapperReduce 中主要有 3 类进程：
* **MRAppMaster**：负责整个 MapReduce 程序的过程调度及状态协调。
* **MapTask**：负责 Map 阶段整个数据处理流程。影响 MapTask 个数（Split 个数）的主要因素有：
    * **文件的大小**：按照块的大小来对划分。`dfs.block.size` （HDFS 块大小）默认为128m。也就是说如果输入文件为 128m 时，会被划分为 1 个 Split；当输入文件为 150m 时，会被划分为 2 个 Split。
    * **文件的个数**：FileInputFormat 按照文件分割 Split，并且只会分割大文件，即那些大小超过 HDFS 块的大小的文件。如果输入的目录中文件有 100 个，那么划分后的 Split 个数至少为 100个。
    * **SplitSize的大小**：分片是按照 SplitSize 的大小进行分割的，一个 Split 的大小在没有设置的情况下，默认等于`dfs.block.size` 的大小。
* **ReduceTask**：负责 Reduce 阶段整个数据处理流程。Reduce 任务是一个数据聚合的步骤，数量默认为 1。使用过多的 Reduce 任务则意味着复杂的 Shuffle，并使输出文件数量激增。而 Reduce 的个数设置相比map的个数设置就要简单的多，只需要在 Driver 类中通过`job.setNumReduceTasks(int n)` 设置即可。

MapTask 数量过多的的话，会产生大量的小文件,过多 MapTask 的创建和初始化都会消耗大量的硬件资源 。
MapTask 数量太小的话，会导致并发度过小，使得 Job 执行时间过长，无法充分利用分布式硬件资源。
可以通过以下方法来控制 MapTask 的数量：
* （1）如果想增加 MapTask 个数，则通过`FileInputFormat.setMaxInputSplitSize(job,long bytes)`方法设置最大数据分片大小为一个小于默认 `dfs.block.size` 的值，越小 MapTask 数量越多。
* （2）如果想减小 MapTask 个数，则通过 `FileInputFormat.setMinInputSplitSize(job,long bytes)`  方法设置最小数据分片大小为一个大于默认 `dfs.block.size` 的值，越大 MapTask 数量越少。
* （3）如果输入中有很多小文件，依然想减少 MapTask 个数，则需要将小文件使用 HDFS 提供的 API 合并为大文件，然后使用方法 2。

## MapReduce 作业执行流程

![](https://chengzw258.oss-cn-beijing.aliyuncs.com/Article/20210619232554.png)

从上面流程图整体可以看出，MapReduce 作业的执行可以分为 11 个步骤，涉及五个独立的实体。它们在 MapReduce 执行过程中的主要作用是：
* **客户端**：提交 MapReduce 作业。
* **YARN 资源管理器（YARN ResourceManager）**：负责协调集群上计算机资源的分配。
* **YARN 节点管理器（YARN NodeManager）**：负责启动和监视集群中机器上的计算容器（Container）。
* **MRAppMaster**：负责协调 MapReduce 作业的任务。MRAppMaster  和 MapReduce 任务运行在容器中，该容器由资源管理器（ResourceManager）进行调度（Schedule）且由节点管理器（NodeManager）进行管理。
* **分布式文件系统（通常是 HDFS）**：用来在其他实体间共享作业文件。

在集群上运行一个 Job 主要分为 6个 大步骤，11 个小步骤，下面将具体内容。
* **一、作业提交**
    * （1）步骤 1 和 2：当提交一个作业时，YARN 会调用用户 API，从资源管理器（ResourceManager）中获得一个 JobID（或 Application ID）。
    * （2）步骤 3：客户端检查作业的输出说明，计算输入分片，并将作业资源（包括 JAR、配置和分片信息）复制到 HDFS 中。
    * （3）步骤 4：客户端调用资源管理器（ResourceManager）上 submitApplication() 方法提交作业。
* **二、作业初始化**
    * （1）步骤 5a 和 5b：当资源管理器（ResourceManager）接收到  submitApplication() 方法的调用，它把请求递交给 YARN 调度器。调度器为节点管理器分配一个容器（Container），资源管理器（ResourceManager）在该容器中启动 MRAppMaster 进程，该进程被节点管理器（NodeManager）管理。
    * （2）步骤 6：MRAppMaster 通过创建一定数量的簿记对象（bookkeeping object）跟踪作业进度来初始化作业，该簿记对象接受任务报告的进度和完成情况。
    * （3）步骤 7：接下来，MRAppMaster 从共享文件系统中获取客户端计算的输入切片，有多少个切片，MapReduce 就会自动创建多少个 Map 任务，而 Reduce 任务的数量受mapreduce.job.reduces 属性控制。
* **三、任务分配**
    * （1）uberized 任务：MRAppMaster 决定如何运行构成 MapReduce 作业的各个任务，当作业比较小时，MRAppMaster 会在一个 JVM 中按顺序运行任务，这样会比在新的容器中运行和分配、并行计算的开销还小，这样的任务就是 uberized 任务。但是在实际生产中，基本不会遇到这样的小任务，MapReduce 只有在处理大量数据的时候才能体现自身的优势。
    * （2）步骤 8：如果作业不适合以 uberized 任务运行，该作业中的所有 Map 任务和 Reduce 任务就会通过 MRAppMaster 向资源管理器请求容器，并通过心跳机制获取返回的心跳信息，该信息包含 block 块机架信息、Map 任务的输入切片位置等，这决定了 Map 获取数据的位置。然后资源调度器根据这些信息进行调度决策，决定将任务分配到哪一个具体的节点以及相应的内存需求。
* **四、任务执行**
   * （1）步骤 9a 和 9b：当资源调度器为 Map 任务和 Reduce 任务分配容器后，MRAppMaster 会发送消息给 NodeManager，并启动容器。
   * （2）步骤 10：任务通过一个主类为 YarnChild 的 Java 应用程序来执行。在它运行任务之前，会向 HDFS 获取作业资源，包括作业的配置信息、JAR 文件和任务操作的文件。
   * （3）步骤 11：YarnChild 获取到了作业资源后，运行 Map 任务或 Reduce 任务。

## Job 运行失败的情况

在测试环境中使用少量数据集进行代码测试可以得到理想结果，而实际情况是，用户代码抛出异常、进程崩溃、机器故障等，主要有以下四种情况失败：
* **1.任务运行失败**：JVM 运行 Map 任务或 Reduce 任务时，可能会出现运行异常而突然退出，此时该任务会反馈给 MRAppMaster 并标记为失败。但这并不意味着该任务已经执行失败，失败的任务会被重新调起执行，在进行 4 次尝试后才会被认为是失败的任务。
* **2.MRAppMaster 运行失败**：MRAppMaster 是通过心跳机制检测运行失败与否，其会定期向资源管理器发送心跳信息。如果 MRAppMaster 发生故障无法发送心跳，资源管理器将检测到该故障并在一个新的容器中开始一个新的 MRAppMaster 实例。新的 MRAppMaster 实例可以获取失败的 MRAppMaster 实例中的任务状态，而不用重新执行一遍所有任务。
* **3.NodeManager 运行失败**：如果 NodeManager 运行失败，就会停止向资源管理器发送心跳信息，并被移除可用节点资源管理器池。如果一个 NodeManager 运行任务失败次数过高，当默认值为 3 次时，那么该 NodeManager 将会被 MRAppMaster 拉入黑名单，该黑名单由 MRAppMaster 管理。
* **4.ResouceManager 运行失败**：ResouceManager 运行失败是非常严重的，我们的 NodeManager、MRAppMaster、作业和任务容器都将无法启动。为了避免出现这种情况，建议在部署生产环境的时候搭建多个 ResourceManager 实现其高可用性。

## wordcount 程序演示
在 MapReduce 组件里，官方给我们提供了一些样例程序，其中非常有名的就是 wordcount 和 pi 程序。这些 MapReduce 程序的代码都在 hadoop-mapreduce-examples-2.7.4.jar 包里，这个 jar 包在Hadoop 安装目录下的 share/hadoop/mapreduce/ 目录里。

下面我们使用 Hadoop 命令来试跑例子程序，看看运行效果。

在 /root 目录下创建 wordcount.txt 文件，内容如下：

```sh
hello hbase
hello hadoop
hello hive
hello kubernetes
hello java
```

在 HDFS 中创建目录并上传文件：
```sh
#在 HDFS 中创建一个目录
hadoop fs -mkdir /wcinput
#将本机 /root/wordcount.txt 文件上传到 HDFS 的 /wcinput 目录中
hadoop fs -put /root/wordcount.txt /wcinput
```

使用以下命令运行 MapReduce 程序计算单词出现次数：

```sh
hadoop jar /software/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.4.jar wordcount /wcinput /wcoutput
```

查看输出结果，可以看到成功计算了每个单词出现的次数：

```sh
[root@hadoop1 ~]# hadoop fs -cat /wcoutput/part-r-00000
hadoop	1
hbase	1
hello	5
hive	1
java	1
kubernetes	1
```

下图说明了 wordcount 例子中 MapReduce 程序执行的流程：
* Input：读取文件内容。
* Split：将每行数据按照空格进行拆分。
* Map：分别计算每行每个单词出现的次数，key 是单词，value 为 1（表示 1 个单词）。
* Shuffle：分区和排序，将 Map 输出中所有 key 相同的部分汇总到一起，作为一个 Reduce 的输入。
* Reduce：把 key 相同的数据进行累计，得到每个单词出现的次数。
* Output：输出结果到文件。

![](https://chengzw258.oss-cn-beijing.aliyuncs.com/Article/20210523195053.png)

## wordcount 代码实现

用户编写的 MapReduce 程序分成三个部分：Mapper，Reducer，Driver：
* 用户自定义 Mapper 类继承 Mapper 类，实现 map() 方法，输出和输出的数据都是 `<K,V>` 对形式，`<K,V>` 类型可以根据实际情况自定义。MapTask 进程对每一个 `<K,V>` 调用一次。
* 用户自定义 Reduce 类继承 Reduce 类，实现 reduce() 方法，输出和输出的数据都是 `<K,V>` 对形式，`<K,V>` 类型可以根据实际情况自定义。Reducetask 进程对每一组相同 K 的 `<K,V>` 组调用一次 reduce() 方法。
* 整个 MapReduce 程序需要一个 Drvier 类来进行提交，提交的是一个描述了各种必要信息的 Job 对象。

### 引入依赖

```xml
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>2.7.4</version>
</dependency>
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>2.7.4</version>
</dependency>
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-mapreduce-client-core</artifactId>
    <version>2.7.4</version>
</dependency>
```
### 编写 Mapper 类

编写一个 WordMapper 类继承 Mapper 类，并重写 map() 方法。Mapper 类是一个泛型类，4 个泛型类型分别代表（KeyIn，ValueIn，KeyOut，ValueOut ）。泛型的类型可以根据自己实际的场景来指定。在 wordcount 这个例子中我们指定的类型如下：
* KeyIn（输入的键）：LongWritable 类型，表示每行文字的起始位置（偏移量）。
* ValueIn（输入的值）：Text 类型，表示每行的文本。
* KeyOut（输出的键）：Text 类型，表示每个单词。
* ValueOut（输出的值为）：LongWritable 类型，表示单词出现的次数（1次）。

Mapper 阶段依次读取每一行的数据，每行按照空格拆分出单词，得到 <单词，1> 的键值对，键是单词，值是 1，之后 Reduce 阶段累计单词出现的次数就累加 1 即可。 

Mapper 阶段代码如下：

```java
package com.chengzw.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author chengzw
 * @description Map 阶段，分别计算每行每个单词出现的次数，key 是单词，value 为 1（表示 1 个单词）。
 * @since 2021/5/17 10:00 下午
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、切分单词
        String[] words = value.toString().split(" ");

        //2、单词转换 单词 -> <单词,1>
        for (String word : words) {
            //3、写入到上下文
            context.write(new Text(word),new LongWritable(1));
        }
    }
}
```

### 编写 Reduce 类

编写一个类 WordReducer 继承 Reducer 类，并重写 reduce() 方法。Reducer 类是也是一个泛型类，4 个泛型类型分别代表（KeyIn，ValueIn，KeyOut，ValueOut ）。泛型的类型可以根据自己实际的场景来指定。在 wordcount 这个例子中我们指定的类型如下：
* KeyIn（输入的键）：Text 类型，表示每个单词。
* ValueIn（输入的值）：LongWritable 类型，表示单词出现的次数（1次）。
* KeyOut（输出的键）：Text 类型，表示每个单词。
* ValueOut（输出的值为）：LongWritable 类型，表示单词出现的总数。

Reduce 阶段接收到数据键是单词，值是一个可迭代的对象，是相同单词对应的次数（每个都是 1），只需要把这些 1 累加起来，就可以得到单词出现的总数了。

```sh
<hello,[1,1,1,1,1]>
```

Reduce 阶段代码如下：

```java
package com.chengzw.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chengzw
 * @description Reduce 阶段，把 key 相同的数据进行累计，得到每个单词出现的次数
 * @since 2021/5/17 10:00 下午
 */
public class WordReducer extends Reducer<Text, LongWritable,Text,LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //1、定义一个变量
        long count = 0;

        //2、迭代
        for (LongWritable value : values) {
            count += value.get();
        }

        //3、写入上下文
        context.write(key,new LongWritable(count));
    }
}
```

### 编写 Driver 类

创建提交给 YARN 集群运行的 Job 对象，其中封装了 MapReduce 程序运行所需要的相关参数，例如输入数据路径，输出数据路径，Mapper 参数，Reduce 参数等。

```java
package com.chengzw.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


/**
 * @description Driver驱动类
 * @author chengzw
 * @since 2021/5/20 8:39 下午
 */

public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //一、初始化Job
        Configuration configuration = new Configuration();

        //获取运行命令的参数，参数一：输入文件路径，参数二：输出文件路径
        //如果输入路径是一个文件，那么只处理这个文件，如果指定的路径是目录，则处理这个目录下的所有文件
        //输出路径只能是不存在的目录名
        String [] otherArgs = new GenericOptionsParser(configuration,args).getRemainingArgs();
        if(otherArgs.length < 2){
            System.err.println("必须提供输入文件路径和输出文件路径");
            System.exit(2);
        }
        Job job = Job.getInstance(configuration, "mr");

        //二、设置Job的相关信息 8个小步骤
        //1、设置输入路径
        job.setInputFormatClass(TextInputFormat.class);
        //本地运行
        //TextInputFormat.addInputPath(job,new Path("/tmp/input/mr.txt"));
        TextInputFormat.addInputPath(job,new Path(args[0]));

        //2、设置Mapper类型，并设置输出键和输出值
        job.setMapperClass(WordMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //shuffle阶段，使用默认的

        //3、设置Reducer类型，并设置输出键和输出值
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //4、设置输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        //本地运行
        //TextOutputFormat.setOutputPath(job,new Path("/tmp/output/mr"));
        TextOutputFormat.setOutputPath(job,new Path(args[1]));


        //三、等待完成
        boolean b = job.waitForCompletion(true);
        System.out.println(b==true?"MapReduce 任务执行成功!":"MapReduce 任务执行失败!");
        System.exit(b ? 0 : 1);
    }
}
```

### 本地测试

编写完代码以后我们可以先在本地进行测试，我们可以在 IntelloiJ IDEA 上设置运行程序时传递的参数（main 方法的 args）。

![](https://chengzw258.oss-cn-beijing.aliyuncs.com/Article/20210619112139.png)

第一个参数是输入的目录路径，该目录下只有一个 mr.txt文件，文件内容如下：

```sh
❯ cat /tmp/input/mr.txt
hello hbase
hello hadoop
hello hive
hello kubernetes
hello java
```
第二个参数是输出的目录路径，这个目录名是不存在的，在运行完 MapReduce 程序后会自动生成该目录（该目录前面的目录不存在也会递归创建）。 


点击 run 运行程序：

![](https://chengzw258.oss-cn-beijing.aliyuncs.com/Article/20210619122346.png)

查看输出结果，可以看到成功统计了每个单词出现的总数。

```sh
❯ cat /tmp/output/mr/part-r-00000
hadoop	1
hbase	1
hello	5
hive	1
java	1
kubernetes	1
```

### 将程序打包为 JAR 包

本地验证程序可以正常运行后，将程序打包为 JAR 包，放到 Hadoop 集群上运行。我们的项目是一个 Maven 项目，点击 Maven -> package 就可以打包了。

![](https://chengzw258.oss-cn-beijing.aliyuncs.com/Article/20210619120724.png)

将生成的 JAR 包拷贝到 Hadoop 机器上：

```sh
scp mapreduce-1.0-SNAPSHOT.jar root@hadoop1:/root
```

### 在 Hadoop 集群上运行 wordcount 程序

使用 hadoop jar 命令运行程序，第一个参数为 JAR 包的路径，第二个参数为 Driver 类全名，第三个参数为输入目录，第四个参数为输出目录。

```sh
[root@hadoop1 ~]# hadoop jar /root/mapreduce.jar com.chengzw.mr.JobMain /wcinput /my_wcoutput
```

运行完成后，查看输出结果：

```sh
[root@hadoop1 ~]# hadoop fs -cat /my_wcoutput/part-r-00000
hadoop  1
hbase   1
hello   5
hive    1
java    1
kubernetes      1
```

## 综合例子

接下来实现一个稍微复杂点的例子，有 order_001 ~ order_006 总共 6 个订单编号，将这些订单分成 3 组，输出到 3 个文件中。然后在每个文件中再根据订单编号进行分组，保留这个订单编号中金额最大的前 3 条记录。

原始数据如下：

```sh
 订单编号     商品编号    金额
order_001   goods_001   100
order_001   goods_002   200
order_002   goods_001   300
order_002   goods_002   400
order_004   goods_003   500
order_003   goods_001   800
order_002   goods_004   500
order_005   goods_002   320
order_001   goods_003   230
order_002   goods_005   730
order_003   goods_003   100
order_006   goods_001   100
order_004   goods_002   350
order_002   goods_001   300
order_006   goods_002   1100
order_006   goods_003   500
order_003   goods_001   800
order_004   goods_004   1200
order_003   goods_002   100
order_005   goods_003   200
order_002   goods_005   700
order_005   goods_003   1300
```

### 编写实体类

编写订单实体类，实现 WritableComparable 接口，我们需要实现 3 个方法：
* **compareTo**：定义排序的规则，按照金额从大到小排序。
* **write**：对象的序列化方法。
* **readFields**：对象的反序列化方法。

```java
package com.chengzw.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author chengzw
 * @description 订单实体类
 * @since 2021/6/11
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId; //订单编号
    private Double price; //订单中某个商品的价格

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "orderId='" + orderId + '\'' +
                ", price=" + price +
                '}';
    }

    /**
     * @param o 实体参数
     * @return 指定排序的规则
     */
    @Override
    public int compareTo(OrderBean o) {
        //1、先比较订单的id，如果id一样，则将订单的商品按金额排序（降序）
        int i = this.orderId.compareTo(o.orderId);
        if (i == 0) {
            //因为是降序，所以有-1
            i = this.price.compareTo(o.price) * - 1;
        }
        return i;
    }

    /**
     * 实现对象的序列化
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    /**
     * 实现对象的反序列化
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.price = in.readDouble();
    }
}
```
### 编写 Mapper 类

Mapper 输入的键是每行文字的起始位置，输入的值是一行文本。
Mapper 输出的键是 OrderBean，输出的值是一行文本。

```java
package com.chengzw.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author chengzw
 * @description Mapper
 * @since 2021/6/11
 */
public class OrderMapper extends Mapper<LongWritable, Text,OrderBean,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1、拆分行文本数据，得到订单的id和订单的金额
        String[] split = value.toString().split(" ");

        //2、封装OrderBean实体类
        OrderBean orderBean = new OrderBean();
        orderBean.setOrderId(split[0]);
        orderBean.setPrice(Double.parseDouble(split[2]));

        //3、写入上下文
        context.write(orderBean,value);
    }
}
```
### 编写 Partition 类（分区）

Partition 和 Group 都是属于 Shuffle 阶段，在 Partition 阶段根据订单编号对数据进行分区，把结果发送给对应的 Reduce 节点并行执行。

```java
package com.chengzw.order;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author chengzw
 * @description 根据订单号分区，输出到不同的文件
 * 分区的目的是根据Key值决定Mapper的输出记录被送到哪一个Reducer上去处理。
 * @since 2021/6/11
 */
public class OrderPartition extends Partitioner<OrderBean, Text> {

    /**
     *
     * @param orderBean k2
     * @param text v2
     * @param numPartitions ReduceTask的个数
     * @return  返回的是分区的编号：比如说：ReduceTask的个数3个，返回的编号是 0 1 2
     *                              比如说：ReduceTask的个数2个，返回的编号是 0 1
     *                              比如说：ReduceTask的个数1个，返回的编号是 0
     */
    @Override
    public int getPartition(OrderBean orderBean, Text text, int numPartitions) {
        //参考源码  return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        //按照key的hash值进行分区，解决int类型数据hashcode值取模出现负数而影响分区
        //key.hashCode() & Integer.MAX_VALUE 是要保证任何map输出的key在numReduceTasks取模后决定的分区为正整数。
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
```
### 编写 Group 类（分组）

将同一个分区内的数据根据订单编号再进行分组。

```java
package com.chengzw.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author chengzw
 * @description  根据订单编号orderId分组，一个分区内可能有多个订单号
 * 实现分组有固定的步骤：
 * 1.继承WritableComparator
 * 2.调用父类的构造器
 * 3.指定分组的规则，重写一个方法
 * @since 2021/6/11
 */
public class OrderGroup extends WritableComparator {

    //1、继承WritableComparator类
    //2、调用父类的构造器
    public OrderGroup(){
        //第一个参数就是分组使用的javabean，第二个参数就是布尔类型，表示是否可以创建这个类的实例
        super(OrderBean.class,true);
    }

    // 3、指定分组的规则，需要重写一个方法
    /**
     * @param a  WritableComparable是接口，Orderbean实现了这个接口
     * @param b WritableComparable是接口，Orderbean实现了这个接口
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //1、对形参a b 做强制类型转换
        OrderBean first = (OrderBean) a;
        OrderBean second = (OrderBean) b;

        //2、指定分组的规则
        return first.getOrderId().compareTo(second.getOrderId());
    }
}
```
### 编写 Reduce 类

Reduce 输入的键的是相同订单编号的 OrderBean 对象，输入的值可迭代对象，是这些 OrderBean 对象对应的一行文本。

由于我们之前在 OrderBean 实体类中定义了排序规则（compareTo()方法），因此这里接收到的 OrderBean 对象是按照金额从大到小的顺序排序的，我们要取相同订单编号 TOP3 的商品，只需要循环遍历输入的值，取前 3 个即可。

输出的键这里直接使用输入的一行文本，输出的值就不需要了，就简单地指定为 NullWritable 类型。

```java
package com.chengzw.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chengzw
 * @description Reduce
 * @since 2021/6/11
 */
public class OrderReducer extends Reducer<OrderBean, Text,Text, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        //OrderBean中实现了compareTo方法，这里的values是已经排好序的了，先比较订单的id，如果id一样，则将订单的商品按金额排序（降序）
        //获取top N ,下面的代码就是取出来top3。
        for (Text value : values) {
            context.write(value,NullWritable.get());
            i++;
            if (i >= 3){
                break;
            }
        }
    }
}
```
### 编写 Driver 类

Driver 类这次除了指定了 Mapper 和 Reduce 参数，还指定了 Shuffle 参数。

```java
package com.chengzw.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author chengzw
 * @description 求订单最大值的主类
 * @since 2021/6/11
 */
public class JobMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //一、初始化一个job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "mygroup");

        //二、配置Job信息
        //1.设置输入信息
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/tmp/input/orders.txt"));

        //2.设置mapper 并设置输出的键和输出的值
        job.setMapperClass(OrderMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(Text.class);

        //3.分区设置，根据订单编号orderId分区，输出到不同的文件
        job.setPartitionerClass(OrderPartition.class);

        //4.分组设置，根据订单编号orderId分区，因为同一个文件中可能有多个订单号
        job.setGroupingComparatorClass(OrderGroup.class);

        //5.设置Reducer,输出的键和输出的值，计算出每个订单号中金额最大的3个商品
        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //设置NumReduceTask（分区）的个数  默认是1
        job.setNumReduceTasks(3);

        //6.设置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("/tmp/output/order"));

        //三、等待完成  实际上就是提交任务
        boolean b = job.waitForCompletion(true);
        System.out.println(b==true?"MapReduce 任务执行成功!":"MapReduce 任务执行失败!");
        System.exit(b ? 0 : 1);
    }
}
```

### 本地测试

本地运行程序：

![](https://chengzw258.oss-cn-beijing.aliyuncs.com/Article/20210619122508.png)

输出有 3 个文件（分区），分别查看 3 个 文件，可以看到根据订单编号分成了 3 个文件，并且每个文件内保留了相同订单编号中金额最大的记录。

```sh
❯ cat /tmp/output/order/part-r-00000
order_002 goods_005 730
order_002 goods_005 700
order_002 goods_004 500
order_005 goods_003 1300
order_005 goods_002 320
order_005 goods_003 200
❯ cat /tmp/output/order/part-r-00001
order_003 goods_001 800
order_003 goods_001 800
order_003 goods_003 100
order_006 goods_002 1100
order_006 goods_003 500
order_006 goods_001 100
❯ cat /tmp/output/order/part-r-00002
order_001 goods_003 230
order_001 goods_002 200
order_001 goods_001 100
order_004 goods_004 1200
order_004 goods_003 500
order_004 goods_002 350
```
## 参考链接
* https://cloud.tencent.com/developer/article/1654079
* https://www.yiibai.com/hadoop/intro-mapreduce.html
* https://juejin.cn/post/6844903687094009863
* https://blog.csdn.net/majianxiong_lzu/article/details/89106290
* https://www.kancloud.cn/liuyw/hadoop_liuyw/1630900