package com.tangxc.hadoop.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * @author Xicheng.Tang
 */
public class HBaseUtils {
    private static Connection connection = null;

    static {
        if (null == connection) {
            Configuration conf = HBaseConfiguration.create();
            conf.set("fs.defaultFS", "hdfs://bigdata-senior:8020");
            conf.set("hbase.zookeeper.quorum", "bigdata-senior");
            conf.set("hbase.client.scanner.timeout.period", "6000");
            conf.set("hbase.rpc.timeout", "6000");
            try {
                connection = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                throw new RuntimeException("connect hbase failed!", e);
            }
        }
    }

    /**
     * 获取Hbase表对象
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    private static Table getTable(String tableName) throws Exception {
        return connection.getTable(TableName.valueOf(tableName));
    }

    public static void getDataByRowkey(String tableName, String columnFamily, String rowKey) throws Exception {
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes(columnFamily));
        Table table = getTable(tableName);
        Result result = table.get(get);
        if (null != result) {
            for (Cell cell : result.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    public static void putData(String tableName, String columnFamily, String rowKey, Map<String, String> dataMap)
            throws Exception {
        Put put = new Put(Bytes.toBytes(rowKey));
        for (Map.Entry<String, String> entry : dataMap.entrySet()) {
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
        }
        getTable(tableName).put(put);
    }

    public static void putData(String tableName, String columnFamily, String rowKey, String columnName, String value)
            throws Exception {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
        getTable(tableName).put(put);
    }

    public static void deleteColumn(String tableName, String columnFamily, String rowKey, String columnName) throws Exception {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        getTable(tableName).delete(delete);
    }

    public static void deleteColumn(String tableName, String columnFamily, String rowKey, Set<String> columnNames) throws Exception {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        for (String columnName : columnNames) {
            delete.addColumns(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        }
        getTable(tableName).delete(delete);
    }

    public static void scan(String tableName, String startRow, String endRow, String... columns) throws Exception {
        Scan scan = new Scan();
        if (null != startRow) {
            scan.setStartRow(Bytes.toBytes(startRow));
        }
        if (null != endRow) {
            scan.setStartRow(Bytes.toBytes(endRow));
        }
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        if (null != columns && columns.length > 0) {
            byte[][] prefixes = new byte[columns.length][];
            for (int i = 0; i < columns.length; i++) {
                prefixes[i] = Bytes.toBytes(columns[i]);
            }
            MultipleColumnPrefixFilter mcpf = new MultipleColumnPrefixFilter(prefixes);
            filterList.addFilter(mcpf);
        }
        scan.setFilter(filterList);
        ResultScanner resultScanner = getTable(tableName).getScanner(scan);
        for (Result result : resultScanner) {
            System.out.println(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
}
