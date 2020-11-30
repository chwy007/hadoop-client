package com.yujizi.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * @Author: ychw
 * @Description:
 * @Date: 2020/11/27 17:52
 */
public class HdClient {

    @Test
    public void test0() throws IOException, InterruptedException {
        Configuration conf=new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop01:9000"), conf, "ycw");
        fs.mkdirs(new Path("/crontab"));
        fs.close();
        System.out.println("over");

    }

    @Test
    public void test01() throws IOException, InterruptedException {
        Configuration conf=new Configuration();
//        conf.set("dfs.replication","2");
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop01:9000"), conf, "ycw");
        fs.copyFromLocalFile(new Path("d:/hadoop/zhangf.txt"),new Path("/universe/earth/xiaohua.txt"));

        fs.close();
        System.out.println("over");



    }

    @Test
    public void test02() throws IOException, InterruptedException {
        Configuration conf=new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop01:9000"), conf, "ycw");
        fs.copyToLocalFile(true,new Path("/zhangf111.txt"),new Path("d:/hadoop/zhangf111.txt"),true);
        fs.close();
        System.out.println("over");



    }

    @Test
    public void test03() throws IOException, InterruptedException {
        Configuration conf=new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop01:9000"), conf, "ycw");
        fs.delete(new Path("/universe/earth/huahua.txt"),true);
        fs.close();
        System.out.println("over");



    }

    @Test
    public void test04() throws IOException, InterruptedException {
        Configuration conf=new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop01:9000"), conf, "ycw");
        fs.rename(new Path("/zhangf11.txt"),new Path("/omg.txt"));
        fs.close();
        System.out.println("over");



    }

    @Test
    public void test05() throws IOException, InterruptedException {
        Configuration conf=new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop01:9000"), conf, "ycw");
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if(fileStatus.isFile()){
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }



        fs.close();
        System.out.println("over");



    }
    

}
