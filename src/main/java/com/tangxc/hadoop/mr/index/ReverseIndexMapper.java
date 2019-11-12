package com.tangxc.hadoop.mr.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author Xicheng.Tang
 */
public class ReverseIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text word = new Text();
    private Text ovalue = new Text();
    private String filePath;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        FileSplit split = (FileSplit) context.getInputSplit();
        filePath = split.getPath().toString();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            ovalue.set(filePath + ":1");
            context.write(word, ovalue);
        }
    }
}
