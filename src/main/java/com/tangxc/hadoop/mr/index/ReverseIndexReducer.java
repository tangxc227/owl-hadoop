package com.tangxc.hadoop.mr.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Xicheng.Tang
 */
public class ReverseIndexReducer extends Reducer<Text, Text, Text, Text>  {
    private Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = null;
        Map<String,Integer> map = new HashMap<>();
        for (Text value : values) {
            String line = value.toString();
            String path = line.substring(0, line.lastIndexOf(":"));
            int count = Integer.parseInt(line.substring(line.lastIndexOf(":") + 1));
            if (map.containsKey(path)) {
                map.put(path, map.get(path) + count);
            } else {
                map.put(path, count);
            }
        }

        sb = new StringBuffer();
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            sb.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
        }

        outputValue.set(sb.deleteCharAt(sb.length() - 1).toString());
        context.write(key, outputValue);
    }
}
