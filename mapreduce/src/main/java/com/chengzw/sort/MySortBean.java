package com.chengzw.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author chengzw
 * @description 实体类
 * @since 2021/5/20
 */
public class MySortBean implements WritableComparable<MySortBean> {

    private String word;
    private int num;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return "MySortBean{" +
                "word='" + word + '\'' +
                ", num=" + num +
                '}';
    }


    /**
     * 比较器，按照定义的规则排序
     * 一行有两个值
         a 1
         b 2
         a 3
         a 3
         c 2
         c 5
         b 6
     * 先按照第一个值排序，第一个值相同时，再按照第二个值排序
     * @param o
     * @return
     */
    @Override
    public int compareTo(MySortBean o) {
        int result = this.word.compareTo(o.word);
        if(result == 0){
            return this.num - o.num;
        }
        return result;
    }

    /**
     * 实现序列化
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(word);
        out.writeInt(num);
    }

    /**
     * 实现反序列化
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {

        this.word = in.readUTF();
        this.num = in.readInt();
    }
}
