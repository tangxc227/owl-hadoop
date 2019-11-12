package com.tangxc.hadoop.hdfs.test;

import com.tangxc.hadoop.hdfs.util.HdfsUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Xicheng.Tang
 */
public class TestCopyToLocal {
    public static void main(String[] args) throws Exception {
        FileSystem fs = HdfsUtils.getFileSystem();
        // hdfs dfs -put
        fs.copyToLocalFile(new Path("/hadoop/api/2.txt"), new Path("D:\\a01.txt"));
        fs.close();
    }
}
