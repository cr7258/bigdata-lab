package com.chengzw.server;

import com.chengzw.api.MyInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @author chengzw
 * @description 服务端
 * @since 2021/7/25
 */
public class RPCServer {
    public static void main(String[] args) {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        //设置 Server 监听的 IP 地址和端口号
        builder.setBindAddress("127.0.0.1");
        builder.setPort(7777);

        builder.setProtocol(MyInterface.class);
        builder.setInstance(new MyInterfaceImpl());

        try {
            RPC.Server build = builder.build();
            build.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
