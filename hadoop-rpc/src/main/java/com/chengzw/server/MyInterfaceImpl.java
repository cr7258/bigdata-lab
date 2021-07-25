package com.chengzw.server;

import com.chengzw.api.MyInterface;
import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author chengzw
 * @description Server 实现协议接口
 * @since 2021/7/25
 */
public class MyInterfaceImpl implements MyInterface {

    Map<Integer,String> studentMap = new HashMap<>();

    public void add(int studentId, String studentName) {
        studentMap.put(studentId,studentName);
        System.out.println("添加学生信息：" + "学生 ID：" + studentId + " 学生姓名：" + studentName);
    }

    public String findName(int studentId) {
        return studentMap.get(studentId);
    }


    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return MyInterface.versionID;
    }

    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return null;
    }
}
