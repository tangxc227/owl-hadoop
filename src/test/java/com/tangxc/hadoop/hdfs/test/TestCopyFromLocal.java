package com.tangxc.hadoop.hdfs.test;

import com.tangxc.hadoop.hdfs.util.HdfsUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Xicheng.Tang
 */
public class TestCopyFromLocal {
    public static void main(String[] args) throws Exception {
        FileSystem fs = HdfsUtils.getFileSystem();
        // hdfs dfs -put
        fs.copyFromLocalFile(new Path("D:\\Program Files\\hadoop-2.6.0\\README.txt"), new Path("/hadoop/api/2.txt"));
        fs.close();
    }
}
