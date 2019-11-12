package com.tangxc.hadoop.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.*;

/**
 * @author Xicheng.Tang
 */
public class UdtfCase extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        if (argOIs.getAllStructFieldRefs().size() != 1) {
            throw new UDFArgumentException("参数异常");
        }
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("id");
        fieldNames.add("name");
        fieldNames.add("price");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        if (args == null || args.length != 1) {
            return;
        }
        // 只有一个参数的情况
        String line = args[0].toString();
        Map<String, String> map = transfoerContent2Map(line);
        List<String> result = new ArrayList<String>();
        result.add(map.get("p_id"));
        result.add(map.get("p_name"));
        result.add(map.get("price"));
        super.forward(result.toArray(new String[0]));
    }

    @Override
    public void close() throws HiveException {
        super.forward(new String[] { "12345689", "close", "123" });
    }

    static Map<String, String> transfoerContent2Map(String content) {
        Map<String, String> map = new HashMap<>();
        int i = 0;
        String key = "";
        StringTokenizer tokenizer = new StringTokenizer(content, "({|}|\"|:|,)");
        while (tokenizer.hasMoreTokens()) {
            if (++i % 2 == 0) {
                // 当前的值是value
                map.put(key, tokenizer.nextToken());
            } else {
                // 当前的值是key
                key = tokenizer.nextToken();
            }
        }
        return map;
    }
}
