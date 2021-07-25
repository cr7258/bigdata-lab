package com.chengzw.client;

import com.chengzw.api.MyInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author chengzw
 * @description 客户端
 * @since 2021/7/25
 */
public class RPCClient {
    public static void main(String[] args) throws IOException {
        MyInterface proxy = RPC.getProxy(MyInterface.class, 1L, new InetSocketAddress("127.0.0.1", 7777), new Configuration());
        proxy.add(1,"chengzw");
        proxy.add(2,"tom");
        proxy.add(3,"jack");
        System.out.println("查询学生：" + proxy.findName(1));
    }
}
