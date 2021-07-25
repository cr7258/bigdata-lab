package com.chengzw.api;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * @author chengzw
 * @description 描述服务对外提供的接口。
 * @since 2021/7/25
 */
public interface MyInterface extends VersionedProtocol {
    static final long versionID = 1L; //版本号，默认情况下，不同版本号的RPC Client和Server之间不能相互通信

    /**
     * 添加学生信息
     * @param StudentId
     * @param StudentName
     * @return
     */
    void add(int StudentId,String StudentName);
    /**
     * 根据学生 ID 获取学生名字
     * @param studentId
     * @return
     */
    String findName(int studentId);
}
