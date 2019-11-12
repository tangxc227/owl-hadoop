package com.tangxc.hadoop.hdfs.test;

import com.tangxc.hadoop.hdfs.util.HdfsUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @author Xicheng.Tang
 */
public class TestMkdirs {
    public static void main(String[] args) throws Exception {
        FileSystem fs = HdfsUtils.getFileSystem();
        //  hdfs dfs -cat ...
        boolean mkdirs = fs.mkdirs(new Path("/hadoop/api/mkdirs"));
        System.out.println(mkdirs ? "创建成功" : "创建失败");
        fs.close();
    }
}
