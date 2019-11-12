package com.tangxc.hadoop.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

/**
 * @author Xicheng.Tang
 * 创建文件并添加内容
 */
public class TestCreate {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata-senior:8020");
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream dos = fs.create(new Path("/hadoop/api/1.txt"), (short) 1);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(dos));
        writer.write("大数据");
        writer.newLine();
        writer.write("flink");
        writer.flush();
        writer.close();
        dos.close();
        fs.close();
    }
}
