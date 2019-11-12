package com.tangxc.hadoop.mr.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Xicheng.Tang
 */
public class ReverseIndexRunner {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata-senior:8020");
        Job job = Job.getInstance(conf, "reverse_index");
        FileInputFormat.setInputPaths(job, "/hadoop/mr/index/input");
        job.setJarByClass(ReverseIndexRunner.class);
        job.setMapperClass(ReverseIndexMapper.class);
        job.setReducerClass(ReverseIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path("/hadoop/mr/index/output" + System.currentTimeMillis()));
        job.waitForCompletion(true);
    }
}
