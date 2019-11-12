package com.tangxc.hadoop.mr.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.bson.Document;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Xicheng.Tang
 */
public class MongoDbInputFormat<V extends MongoDbWritable> extends InputFormat<LongWritable, V> {

    /**
     * MongoDB自定义InputSplit
     */
    static class MongoDbInputSplit extends InputSplit implements Writable {

        private long start;
        private long end;

        public MongoDbInputSplit() {
            super();
        }

        public MongoDbInputSplit(long start, long end) {
            super();
            this.start = start;
            this.end = end;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(this.start);
            dataOutput.writeLong(this.end);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.start = dataInput.readLong();
            this.end = dataInput.readLong();
        }

        @Override
        public long getLength() throws IOException, InterruptedException {
            return this.end - this.start;
        }

        @Override
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }
    }

    /**
     * 获取分片信息
     *
     * @param jobContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        // 获取mongo连接
        MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);
        MongoDatabase mongoDatabase = mongoClient.getDatabase("hadoop");
        MongoCollection<Document> collection = mongoDatabase.getCollection("persons");
        // 每两条数据一个mapper
        int chunkSize = 2;
        // 获取mongodb对应collection的数据条数
        long size = collection.count();
        // 计算mapper个数
        long chunk = size / chunkSize;
        List<InputSplit> list = new ArrayList<>();
        for (int i = 0; i < chunk; i++) {
            if (i + 1 == chunk) {
                list.add(new MongoDbInputSplit(i * chunkSize, size));
            } else {
                list.add(new MongoDbInputSplit(i * chunkSize, i * chunkSize + chunkSize));
            }
        }
        return list;
    }

    /**
     * 获取具体的reader类
     *
     * @param inputSplit
     * @param taskAttemptContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<LongWritable, V> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MongoDbRecordReader(inputSplit, taskAttemptContext);
    }

    static class NullMongoDbWritable implements MongoDbWritable {

        @Override
        public void readFields(Document document) {

        }

        @Override
        public void write(MongoCollection<Document> collection) {

        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {

        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {

        }
    }

    static class MongoDbRecordReader<V extends MongoDbWritable> extends RecordReader<LongWritable, V> {

        private MongoDbInputSplit split;
        private LongWritable key;
        private V value;
        private int index;
        private MongoCursor<Document> cursor;

        public MongoDbRecordReader() {
            super();
        }

        public MongoDbRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            super();
            this.initialize(inputSplit, context);
        }

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            this.split = (MongoDbInputSplit) inputSplit;
            Configuration conf = context.getConfiguration();
            key = new LongWritable();
            Class clz = conf.getClass("mapreduce.mongo.split.value.class", NullMongoDbWritable.class);
            value = (V) ReflectionUtils.newInstance(clz, conf);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (this.cursor == null) {
                MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);
                MongoDatabase mongoDatabase = mongoClient.getDatabase("hadoop");
                MongoCollection<Document> collection = mongoDatabase.getCollection("persons");
                this.cursor = collection.find().skip((int) this.split.start).limit((int) this.split.getLength()).iterator();
            }
            boolean hasNext = cursor.hasNext();
            if (hasNext) {
                Document document = cursor.next();
                System.out.println(document);
                this.key.set(this.split.start + index);
                this.index++;
                this.value.readFields(document);
            }
            return hasNext;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return this.key;
        }

        @Override
        public V getCurrentValue() throws IOException, InterruptedException {
            return this.value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void close() throws IOException {

        }
    }
}
