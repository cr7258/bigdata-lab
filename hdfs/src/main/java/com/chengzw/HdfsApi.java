package com.chengzw;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author 程治玮
 * @since 2021/5/12 10:53 上午
 */
public class HdfsApi {

    public static void main(String[] args) throws Exception {

        HdfsApi hdfsApi = new HdfsApi();

        FileSystem fileSystem = hdfsApi.getFileSystem2();

        //遍历文件
        hdfsApi.listFiles(fileSystem);

        //创建文件夹
        //hdfsApi.mkdirs(fileSystem);

        //上传文件
        //hdfsApi.uploadFile(fileSystem);

        //下载文件
        //hdfsApi.downloadFile(fileSystem);

        //合并上传
        //hdfsApi.mergeUpload(fileSystem);

        //合并下载
        //hdfsApi.mergeDownload(fileSystem);
    }

    /**
     * 获取FileSystem
     */
    //方法一
    public FileSystem getFileSystem1() throws IOException {
        //1.创建Configuration对象
        Configuration conf = new Configuration();
        //2.设置文件系统类型，root 是
        conf.set("fs.defaultFS", "hdfs://hadoop1:8020");
        //3.获取HDFS文件系统文件系统
        FileSystem fileSystem = FileSystem.get(conf);

        return fileSystem;
    }

    //方法二
    public FileSystem getFileSystem2() throws IOException, URISyntaxException, InterruptedException {
        //chengzw用户是hadoop中的用户，Linux系统上可以不存在该用户
        //使用hadoop fs -chown 命令给目录或文件授权
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop1:8020"), new Configuration(), "root");
        return fileSystem;
    }

    //方法三
    public FileSystem getFileSystem3() throws IOException {
        //1:创建Configuration对象
        Configuration conf = new Configuration();

        //2:设置文件系统类型
        conf.set("fs.defaultFS", "hdfs://hadoop1:8020");

        //3:获取HDFS文件系统
        FileSystem fileSystem = FileSystem.newInstance(conf);

        return fileSystem;
    }

    //方法四
    public FileSystem getFileSystem4() throws Exception {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://hadoop1:8020"), new Configuration());
        return fileSystem;
    }

    /**
     * 文件遍历
     * hadoop fs -ls -R /
     */
    public void listFiles(FileSystem fileSystem) throws IOException {
        //获取迭代器
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/tmp"), false);
        //迭代输出文件
        while (iterator.hasNext()) {
            LocatedFileStatus fileStatus = iterator.next();
            System.out.println(fileStatus.getPath() + "====" + fileStatus.getPath().getName());
        }
    }

    /**
     * 创建文件夹
     * hadoop fs -mkdir -p /aa/bb/cc
     */
    public void mkdirs(FileSystem fileSystem) throws IOException {
        //创建文件夹
        fileSystem.mkdirs(new Path("/aa/bb/cc"));
        //关闭FileSystem
        fileSystem.close();
    }

    /**
     * 上传文件
     * hadoop fs -put /tmp/upload.txt /tmp/hdfs-upload.txt
     */
    public void uploadFile(FileSystem fileSystem) throws IOException {
        //上传文件，将本地/tmp/upload.txt文件上传到hdfs的/tmp目录下，文件名为hdfs-upload.txt，如果不指定文件名将和上传的文件名一致
        fileSystem.copyFromLocalFile(new Path("/tmp/upload.txt"), new Path("/tmp/hdfs-upload.txt"));
        //关闭FileSystem
        fileSystem.close();
    }

    /**
     * 下载文件
     * hadoop fs -get /tmp/hdfs-upload.txt /tmp
     */
    public void downloadFile(FileSystem fileSystem) throws IOException {
        //获取HDFS文件输入流
        FSDataInputStream inputStream = fileSystem.open(new Path("/tmp/hdfs-upload.txt"));
        //获取本地文件输出流，需要制定下载文件名
        FileOutputStream outputStream = new FileOutputStream("/tmp/hdfs-download.txt");
        //拷贝文件
        IOUtils.copy(inputStream, outputStream);
        //关闭输出流，输入流，HDFS文件系统
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
    }

    /**
     * 合并上传
     * hadoop fs -appendToFile /tmp/file* /tmp/file_merge
     */
    public void mergeUpload(FileSystem fileSystem) throws IOException {
        //获取HDFS文件输出流
        FSDataOutputStream outputStream = fileSystem.create(new Path("/tmp/file-merge-upload"));
        //获取本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        //获取本地文件夹下所有文件的详情
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("/tmp"));
        //遍历每个文件，获取每个文件输入流
        for (FileStatus fileStatus : fileStatuses) {
            Path filePath = fileStatus.getPath();
            //如果文件名前面是file开头，并且是文件
            if ("file".equals(filePath.getName().substring(0, 4)) && fileStatus.isFile()) {
                System.out.println("合并上传文件: " + filePath.getName());
                FSDataInputStream inputStream = localFileSystem.open(filePath);
                //合并小文件
                IOUtils.copy(inputStream, outputStream);
                //关闭文件输入流
                IOUtils.closeQuietly(inputStream);
            }
        }
        //关闭文件输出流，本地文件系统，HDFS文件系统
        IOUtils.closeQuietly(outputStream);
        localFileSystem.close();
        fileSystem.close();
    }

    /**
     * 合并下载
     * hadoop fs -getmerge /tmp/file* /tmp/file_merge
     */
    public void mergeDownload(FileSystem fileSystem) throws IOException {
        //获取本地文件输出流
        FileOutputStream outputStream = new FileOutputStream("/tmp/file-merge-download");
        //获取HDFS文件
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/tmp"), false);
        //遍历每个文件，获取每个文件输入流
        while (iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();
            //如果文件名前面是file开头，并且是文件
            if("file".equals(fileStatus.getPath().getName().substring(0,4))){
                Path filePath = fileStatus.getPath();
                Path fileName = new Path("/" + filePath.getParent().getName() + "/" + filePath.getName());
                FSDataInputStream inputStream = fileSystem.open(fileName);
                //合并小文件
                IOUtils.copy(inputStream,outputStream);
                //关闭文件输入流
                IOUtils.closeQuietly(inputStream);
            }
        }
        //关闭文件输出流，HDFS文件系统
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
    }
}
