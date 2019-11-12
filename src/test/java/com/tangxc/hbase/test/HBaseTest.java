package com.tangxc.hbase.test;

import com.tangxc.hadoop.hbase.util.HBaseUtils;

/**
 * @author Xicheng.Tang
 */
public class HBaseTest {
    public static void main(String[] args) throws Exception {
        HBaseUtils.scan("online_product", null, null);
    }
}
