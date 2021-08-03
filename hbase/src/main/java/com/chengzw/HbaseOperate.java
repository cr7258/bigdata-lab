package com.chengzw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chengzw
 * @description
 * @since 2021/7/29
 */
public class HbaseOperate {

    public static void main(String[] args) throws Throwable {


        //创建命名空
        System.out.println("===========创建命名空间===========");
        createNamespace("chengzw");

        //获取命名空间
        System.out.println("===========获取命名空间===========");
        String[] namespaces = listNamespace();
        for (String namespace : namespaces) {
            System.out.println(namespace);
        }

        //检查表是否存在
        System.out.println("===========检查表是否存在===========");
        System.out.println(isTableExist("chengzw:student"));

        //创建表
        System.out.println("===========创建表===========");
        createTable("chengzw:student","info","score");

        //列出表
        System.out.println("===========列出表===========");
        TableName[] tableNames = listTable();
        for (TableName tableName : tableNames) {
            System.out.println(tableName);
        }

        //插入数据
        System.out.println("===========插入数据===========");
        addData("chengzw:student","Tom","info","student_id","20210000000001");
        addData("chengzw:student","Tom","info","class","1");
        addData("chengzw:student","Tom","score","understanding","75");
        addData("chengzw:student","Tom","score","programming","82");

        addData("chengzw:student","Jerry","info","student_id","20210000000002");
        addData("chengzw:student","Jerry","info","class","1");
        addData("chengzw:student","Jerry","score","understanding","85");
        addData("chengzw:student","Jerry","score","programming","67");

        addData("chengzw:student","Jack","info","student_id","20210000000003");
        addData("chengzw:student","Jack","info","class","2");
        addData("chengzw:student","Jack","score","understanding","80");
        addData("chengzw:student","Jack","score","programming","80");

        addData("chengzw:student","Rose","info","student_id","20210000000004");
        addData("chengzw:student","Rose","info","class","2");
        addData("chengzw:student","Rose","score","understanding","60");
        addData("chengzw:student","Rose","score","programming","61");

        addData("chengzw:student","程治玮","info","student_id","G20210735010497");
        addData("chengzw:student","程治玮","info","class","5");
        addData("chengzw:student","程治玮","score","understanding","100");
        addData("chengzw:student","程治玮","score","programming","100");

        //扫描表所有数据
        System.out.println("===========扫描表所有数据===========");
        scanAllData("chengzw:student");

        //扫描指定列
        System.out.println("===========扫描指定列===========");
        scanQualifierData("chengzw:student","score","programming");

        //扫描指定列在指定行键范围的值
        System.out.println("===========扫描指定列在指定行范围的值===========");
        scanQualifierRowData("chengzw:student","info","student_id","Jack","Rose");

        //按行键读取行
        System.out.println("===========按行键读取行===========");
        getDataByRowkey("chengzw:student","Tom");


        //获取某行指定列的值
        System.out.println("===========获取某行指定列的数据===========");
        getRowQualifierData("chengzw:student","Tom","score","programming");

        //获取行数
        System.out.println("===========获取行数===========");
        countRows("chengzw:student");

        //获取某列的值
        System.out.println("===========删除一行或多行===========");
        deleteRowsData("chengzw:student", "Tom","Jack");

        //删除表
        System.out.println("===========删除表===========");
        dropTable("chengzw:student");

        //删除命名空间
        System.out.println("===========删除命名空间===========");
        dropNamespace("chengzw");
    }

    //获取Configuration对象
    public static Configuration conf;
    public static Connection connection;
    //HBaseAdmin： HBase系统管理员的角色，对数据表进行操作或者管理
    public static HBaseAdmin admin;

    static {
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
     *
     * @param namespace
     * @throws IOException
     */
    public static void createNamespace(String namespace) throws IOException {
        //创建namespace
        NamespaceDescriptor descriptor = NamespaceDescriptor.create(namespace).build();
        admin.createNamespace(descriptor);
    }

    /**
     * 列出命名空间
     *
     * @return
     * @throws IOException
     */
    public static String[] listNamespace() throws IOException {
        return admin.listNamespaces();
    }

    /**
     * 创建表，String... columnFamily 是不定长参数
     *
     * @param tableName
     * @param columnFamily
     * @throws Exception
     */
    public static void createTable(String tableName, String... columnFamily) throws Exception {
        //判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("table： " + tableName + " 表已存在");
        } else {
            //创建表属性对象,表名需要转字节类型
            //HTableDescriptor：所有列簇的信息（一到多个HColumnDescriptor）
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //建表的时候要添加这行，否则下面统计行数的时候会报错：
            //org.apache.hadoop.hbase.exceptions.UnknownProtocolException: No registered coprocessor service found for name AggregateService in region
            descriptor.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation");

            //创建多个列簇
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("table： " + tableName + " 表创建成功！");
        }
    }

    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    public static boolean isTableExist(String tableName) throws Exception {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * 列出表
     *
     * @return
     * @throws IOException
     */
    public static TableName[] listTable() throws IOException {
        return admin.listTableNames();
    }


    /**
     * 插入数据
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param qualifier
     * @param value
     * @throws IOException
     */
    public static void addData(String tableName, String rowKey, String columnFamily, String qualifier, String value) throws IOException {
        //获取table对象，对表的数据进行增删改查操作
        Table table = connection.getTable(TableName.valueOf(tableName));
        //插入一行数据
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        table.put(put);
        System.out.println("插入数据成功: 行键:" + rowKey + ", 列簇:" + columnFamily + ", 列:" + qualifier + ", 值:" + value);
        table.close();
    }

    /**
     * 扫描表查看数据
     *
     * @param tableName
     * @throws IOException
     */
    public static void scanAllData(String tableName) throws IOException {
        //获取table对象，对表的数据进行增删改查操作
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        //使用HTable得到resultcanner实现类的对象
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            //Cell：封装了Column的所有的信息：Rowkey、column qualifier、value、时间戳
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        ", 列簇:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ", 列:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ", 值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }

    /**
     * 扫描指定列
     *
     * @param tableName
     * @param columnFamily
     * @param qualifier
     * @throws IOException
     */
    public static void scanQualifierData(String tableName, String columnFamily, String qualifier) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        //扫描指定的列
        scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            //Cell：封装了Column的所有的信息：Rowkey、column qualifier、value、时间戳
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        ", 列簇:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ", 列:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ", 值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }


    /**
     * 扫描指定列在指定行键范围的值
     *
     * @param tableName
     * @param columnFamily
     * @param qualifier
     * @param startRow
     * @param stopRow
     * @throws IOException
     */
    public static void scanQualifierRowData(String tableName, String columnFamily, String qualifier, String startRow, String stopRow) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        //指定起始行键和结束行键
        Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(stopRow));
        //扫描指定的列
        scan.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            //Cell：封装了Column的所有的信息：Rowkey、column qualifier、value、时间戳
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                        ", 列簇:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ", 列:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ", 值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }

    /**
     * 按行键读取行
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getDataByRowkey(String tableName, String rowKey) throws IOException {
        //获取table对象，对表的数据进行增删改查操作
        Table table = connection.getTable(TableName.valueOf(tableName));
        Result result = table.get(new Get(Bytes.toBytes(rowKey)));
        for (Cell cell : result.rawCells()) {
            System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                    ", 列簇:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    ", 列:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    ", 值:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table.close();
    }

    /**
     * 获取某行指定列的值
     *
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param qualifier
     * @throws IOException
     */
    public static void getRowQualifierData(String tableName, String rowKey, String columnFamily, String qualifier) throws IOException {
        //获取table对象，对表的数据进行增删改查操作
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        //获取某行指定列的值
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)) +
                    ", 列簇:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    ", 列:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    ", 值:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table.close();
    }

    /**
     * 获取行数
     *
     * @param tableName
     * @throws Throwable
     */
    public static void countRows(String tableName) throws Throwable {
        Scan scan = new Scan();
        AggregationClient aggregationClient = new AggregationClient(conf);
        System.out.println("RowCount: " + aggregationClient.rowCount(TableName.valueOf(tableName),  new LongColumnInterpreter(), scan));
    }


    /**
     * 删除一行或多行数据
     *
      * @param tableName
     * @param rows
     * @throws IOException
     */
    public static void deleteRowsData(String tableName, String... rows) throws IOException{
        //获取table对象，对表的数据进行增删改查操作
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList = new ArrayList<Delete>();
        //根据行键循环
        for(String row : rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        table.delete(deleteList);
        table.close();
    }

    /**
     * 删除表
     *
     * @param tableName
     * @throws IOException
     */
    public static void dropTable(String tableName) throws IOException {
        //删除前先要禁用表
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * 删除命名空间
     *
     * @param namespace
     * @throws IOException
     */
    public static void dropNamespace(String namespace) throws IOException {
        admin.deleteNamespace(namespace);
    }

}
