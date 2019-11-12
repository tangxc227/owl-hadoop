package com.tangxc.hadoop.hdfs.test;

import com.tangxc.hadoop.hdfs.util.HdfsUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @author Xicheng.Tang
 */
public class TestDelete {
    public static void main(String[] args) throws Exception {
        FileSystem fs = HdfsUtils.getFileSystem();
        fs.delete(new Path("/hadoop/api/mkdirs"), true);
        fs.close();
    }
}
