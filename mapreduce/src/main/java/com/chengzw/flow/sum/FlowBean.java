package com.chengzw.flow.sum;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @description 流量flowbean实体类
 * @author chengzw
 * @since 2021/5/20 8:39 下午
 *
| 上行数据包 | upFlow        | int    |
| 下行数据包 | downFlow      | int    |
| 上行流量   | upCountFlow   | int    |
| 下行流量   | downCountFlow | int    |
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private Integer upFlow; //上行数据包
    private Integer downFlow; //下行数据包
    private Integer upCountFlow;  //上行流量
    private Integer downCountFlow; //下行流量

    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    public Integer getUpCountFlow() {
        return upCountFlow;
    }

    public void setUpCountFlow(Integer upCountFlow) {
        this.upCountFlow = upCountFlow;
    }

    public Integer getDownCountFlow() {
        return downCountFlow;
    }

    public void setDownCountFlow(Integer downCountFlow) {
        this.downCountFlow = downCountFlow;
    }

    @Override
    public String toString() {
        return upFlow +
                "\t" + downFlow +
                "\t" + upCountFlow +
                "\t" + downCountFlow;
    }


    /**
     * 比较器，按照定义的规则排序
     * 按照upFlow流量倒排
     * @param o
     * @return
     */
    @Override
    public int compareTo(FlowBean o) {
        return o.upFlow - this.upFlow;
    }


    /**
     * 序列化
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeInt(upCountFlow);
        out.writeInt(downCountFlow);
    }

    /**
     * 反序列化
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readInt();
        this.downFlow = in.readInt();
        this.upCountFlow = in.readInt();
        this.downCountFlow = in.readInt();
    }
}