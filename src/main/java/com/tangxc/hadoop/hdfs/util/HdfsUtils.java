package com.tangxc.hadoop.hdfs.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * @author Xicheng.Tang
 */
public class HdfsUtils {

    /**
     * 获取hadoop configuration对象
     *
     * @return
     */
    public static Configuration getConf() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata-senior:8020");
        return conf;
    }

    public static FileSystem getFileSystem() {
        return getFileSystem(getConf());
    }

    public static FileSystem getFileSystem(Configuration conf) {
        try {
            return FileSystem.get(conf);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取FileSystem对象失败...", e);
        }
    }

}
