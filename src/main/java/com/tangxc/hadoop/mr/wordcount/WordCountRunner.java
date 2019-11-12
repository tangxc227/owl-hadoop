package com.tangxc.hadoop.mr.wordcount;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Xicheng.Tang
 */
public class WordCountRunner implements Tool {

    private Configuration conf = null;

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
        this.conf.set("fs.defaultFS", "hdfs://bigdata-senior:8020");
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "wordcount");
        // 1. 输入
        FileInputFormat.addInputPath(job, new Path("/hadoop/mr/wordcount/input"));
        // 2. map
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // 3. shuffle
        // 4. reduce
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // 5. output
        FileOutputFormat.setOutputPath(job, new Path("/hadoop/mr/wordcount/output"));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // 运行
        ToolRunner.run(new WordCountRunner(), args);
    }
}
