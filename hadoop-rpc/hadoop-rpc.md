# Hadoop RPC

## Hadoop RPC 调用流程和原理

1. Client 和 Server 端的通过 Socket 连接进行通讯。
2. 客户端通过 RPC.getProxy 方法获取代理对象，执行方法的时候，代理对象拦截调用的方法，拿到方法名称，参数序列
化之后通过 Socket 发给 Server，Server 反序列化得到相应的参数调用具体的实现对象。

![](https://chengzw258.oss-cn-beijing.aliyuncs.com/Article/20210725123719.png)

## 实现步骤

1.定义 RPC 接口：定义服务器端对外提供的服务接口。
2.实现 RPC 接口：Server 端实现该接口。
3.构造和启动 RPC SERVER，通过构造器 Builder 构造一个 RPC Server，并调用 start() 方法启动该 Server。
4.构造 RPC Client 发送请求，使用静态方法 RPC.getProxy 构造客户端代理对象，直接通过代理对象调用远程 Server 端的方法。

## 验证

启动 RPCClient 添加 3 个学生信息，在 RPCServer 端输出如下：
```sh
添加学生信息：学生 ID：1 学生姓名：chengzw
添加学生信息：学生 ID：2 学生姓名：tom
添加学生信息：学生 ID：3 学生姓名：jack
```

RPCClient 查询 studentId 为 1 的学生，返回内容如下：
```sh
查询学生：chengzw
```