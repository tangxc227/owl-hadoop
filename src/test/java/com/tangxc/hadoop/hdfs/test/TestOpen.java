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
public class TestOpen {
    public static void main(String[] args) throws Exception {
        FileSystem fs = HdfsUtils.getFileSystem();
        //  hdfs dfs -cat ...
        FSDataInputStream dis = fs.open(new Path("/hadoop/api/1.txt"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(dis));
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        reader.close();
        dis.close();
        fs.close();
    }
}
