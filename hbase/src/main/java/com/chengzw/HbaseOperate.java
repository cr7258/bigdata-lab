package com.chengzw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author chengzw
 * @description
 * @since 2021/7/29
 */
public class HbaseOperate {

    public static void main(String[] args) throws Exception {


        //创建命名空
        //System.out.println("===========创建命名空间===========");
        //createNamespace("chengzw");

        //获取命名空间
        //System.out.println("===========获取命名空间===========");
        //String[] namespaces = listNamespace();
        //for (String namespace : namespaces) {
        //    System.out.println(namespace);
        //}

        //检查表是否存在
        //System.out.println("===========检查表是否存在===========");
        //System.out.println(isTableExist("chengzw:student"));

        //创建表
        //System.out.println("===========创建表===========");
        //createTable("chengzw:student","name","info","score");

        //列出表
        //System.out.println("===========列出表===========");
        //TableName[] tableNames = listTable();
        //for (TableName tableName : tableNames) {
        //    System.out.println(tableName);
        //}

        //扫描表所有数据
        System.out.println("===========扫描表所有数据===========");
        scanAllData("chengzw:studnet");

        //获取某列的值
        //System.out.println();
        //getColumnData("")
        //createTable("student2","base_info","extra_info");
        //dropTable("student2");
        //addData("student1", "002", "info", "name", "zhangsan");//注意增加数据，存在就是修改，不存在就是增加
        //addData("student1", "002", "info", "age", "30");
        //addData("student1", "002", "info", "school", "BUPT");
        //getAllData("student1");
        //getRowData("student1","001");
        //getRowQualifierData("student1", "001", "info", "school");
        //deleteRowsData("student1", "002");
        //getAllData("student7");
    }

    //获取Configuration对象
    public static Configuration conf;
    public static Connection connection;
    //HBaseAdmin： HBase系统管理员的角色，对数据表进行操作或者管理
    public static HBaseAdmin admin;
    static{
        //使用HBaseConfiguration的单例方法实例化
        //HBaseConfiguration：封装了hbase集群所有的配置信息（最终代码运行所需要的各种环境）
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181");
        try {
            //创建连接
            connection = ConnectionFactory.createConnection(conf);
            //创建HBaseAdmin对象，用于对表进行create,delete,list等操作
            admin = (HBaseAdmin) connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 创建命名空间
     * @param namespace
     * @throws IOException
     */
    public static void createNamespace(String namespace) throws IOException {
        //创建namespace
        NamespaceDescriptor descriptor =  NamespaceDescriptor.create(namespace).build();
        admin.createNamespace(descriptor);
    }

    /**
     * 列出命名空间
     * @return
     * @throws IOException
     */
    public static String[] listNamespace() throws IOException {
        return admin.listNamespaces();
    }

    /**
     * 创建表，String... columnFamily 是不定长参数
     * @param tableName
     * @param columnFamily
     * @throws Exception
     */
    public static void createTable(String tableName, String... columnFamily) throws Exception{
        //判断表是否存在
        if(isTableExist(tableName)){
            System.out.println("table： " + tableName + " 表已存在");
        }else{
            //创建表属性对象,表名需要转字节类型
            //HTableDescriptor：所有列簇的信息（一到多个HColumnDescriptor）
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列簇
            for(String cf : columnFamily){
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("table： " + tableName + " 表创建成功！");
        }
    }

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     * @throws Exception
     */
    public static boolean isTableExist(String tableName) throws Exception {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * 列出表
     * @return
     * @throws IOException
     */
    public static TableName[] listTable() throws IOException {
        return admin.listTableNames();
    }


    //public static void addData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
    //    //创建HTable对象
    //    HTable hTable = new HTable(conf, tableName);
    //    //向表中插入数据
    //    Put put = new Put(Bytes.toBytes(rowKey));
    //    //向Put对象中组装数据
    //    put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
    //    hTable.put(put);
    //    hTable.close();
    //    System.out.println("插入数据成功");
    //}

    /**
     * 扫描表查看数据
     * @param tableName
     * @throws IOException
     */
    //public static void scanAllData(String tableName) throws IOException{
    //    //HTable：封装了整个表的所有的信息（表名，列簇的信息），提供了操作该表数据所有的业务方法。
    //    HTable hTable = new HTable(connection,conf, tableName);
    //    //得到用于扫描region的对象scan
    //    //Scan： 封装查询信息，很get有一点不同，Scan可以设置Filter
    //    Scan scan = new Scan();
    //    //使用HTable得到resultcanner实现类的对象
    //    ResultScanner resultScanner = hTable.getScanner(scan);
    //    for(Result result : resultScanner){
    //        //Cell：封装了Column的所有的信息：Rowkey、column qualifier、value、时间戳
    //        Cell[] cells = result.rawCells();
    //        for(Cell cell : cells){
    //            System.out.println("行键: " + Bytes.toString(CellUtil.cloneRow(cell)));
    //            System.out.println("列簇: " + Bytes.toString(CellUtil.cloneFamily(cell)));
    //            System.out.println("列: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
    //            System.out.println("值: " + Bytes.toString(CellUtil.cloneValue(cell)));
    //            System.out.println();
    //        }
    //    }
    //}

    //public static void getColumnData(String columnFamily, String column) throws IOException {
    //    Scan scan = new Scan();
    //    scan.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column));
    //    ResultScanner resultScanner = hTable.getScanner(scan);
    //    for(Result result : resultScanner){
    //        //Cell：封装了Column的所有的信息：Rowkey、column qualifier、value、时间戳
    //        Cell[] cells = result.rawCells();
    //        for(Cell cell : cells){
    //            System.out.println("行键: " + Bytes.toString(CellUtil.cloneRow(cell)));
    //            System.out.println("列簇: " + Bytes.toString(CellUtil.cloneFamily(cell)));
    //            System.out.println("列: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
    //            System.out.println("值: " + Bytes.toString(CellUtil.cloneValue(cell)));
    //            System.out.println();
    //        }
    //    }
    //}
}
