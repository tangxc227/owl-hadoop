package com.tangxc.hadoop.mr.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.bson.Document;

import java.io.IOException;

/**
 * @author Xicheng.Tang
 */
public class MongoDbOutputFormat<V extends MongoDbWritable> extends OutputFormat<NullWritable, V> {

    static class MongoDbRecordWriter<V extends MongoDbWritable> extends RecordWriter<NullWritable, V> {
        private MongoCollection<Document> collection = null;

        public MongoDbRecordWriter() {
        }

        public MongoDbRecordWriter(TaskAttemptContext context) throws IOException {
            MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);
            MongoDatabase mongoDatabase = mongoClient.getDatabase("hadoop");
            collection = mongoDatabase.getCollection("result");
        }

        @Override
        public void write(NullWritable key, V value) throws IOException, InterruptedException {
            value.write(this.collection);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            // 没有关闭方法
        }
    }

    @Override
    public RecordWriter<NullWritable, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MongoDbRecordWriter<>(context);
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitter(null, context);
    }
}
