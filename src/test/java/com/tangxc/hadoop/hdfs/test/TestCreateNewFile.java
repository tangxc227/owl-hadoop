package com.tangxc.hadoop.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Xicheng.Tang
 * 创建一个空文件
 */
public class TestCreateNewFile {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata-senior:8020");
        FileSystem fs = FileSystem.get(conf);
        boolean created = fs.createNewFile(new Path("/hadoop/api/createNewFile.txt"));
        System.out.println(created ? "创建成功" : "创建失败");
        fs.close();
    }
}
