package com.yujizi.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

/**
 * @Author: ychw
 * @Description:
 * @Date: 2020/11/27 21:53
 */
public class HDFSio {

    @Test
    public void test04() throws IOException, InterruptedException {
        Configuration conf=new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop01:9000"), conf, "ycw");

        FileInputStream fileInputStream = new FileInputStream(new File("d:/hadoop/zhangf.txt"));
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/mary.txt"));
        IOUtils.copyBytes(fileInputStream,fsDataOutputStream,conf);


        IOUtils.closeStream(fileInputStream);
        IOUtils.closeStream(fsDataOutputStream);
        fs.close();
        System.out.println("over");



    }
}
