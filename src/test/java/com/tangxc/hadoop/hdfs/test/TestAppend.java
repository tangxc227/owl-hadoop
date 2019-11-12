package com.tangxc.hadoop.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Xicheng.Tang
 * 向文件追加内容
 */
public class TestAppend {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata-senior:8020");
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream dos = fs.append(new Path("/hadoop/api/createNewFile.txt"), 4096);
        dos.write("大数据".getBytes());
        dos.flush();
        dos.close();
        fs.close();
    }
}
