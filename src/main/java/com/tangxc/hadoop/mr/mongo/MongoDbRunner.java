package com.tangxc.hadoop.mr.mongo;

import com.mongodb.client.MongoCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.Document;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Xicheng.Tang
 */
public class MongoDbRunner {
    /**
     * mongo数据转换到hadoop的一个bean
     */
    static class PersonMongoDbWritable implements MongoDbWritable {
        private String name;
        private Integer age;
        private String sex = "";
        private int count = 1;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.name);
            out.writeUTF(this.sex);

            // 写出有数据可能为空的情况
            if (this.age == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeInt(this.age);
            }
            out.writeInt(this.count);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.name = in.readUTF();
            this.sex = in.readUTF();
            if (in.readBoolean()) {
                this.age = in.readInt();
            } else {
                this.age = null;
            }
            this.count = in.readInt();
        }

        @Override
        public void readFields(Document document) {
            this.name = document.get("name").toString();
            if (document.get("age") != null) {
                this.age = Double.valueOf(document.get("age").toString()).intValue();
            } else {
                this.age = null;
            }
        }

        @Override
        public void write(MongoCollection<Document> collection) {
            Document document = new Document("age", this.age).append("count", this.count);
            collection.insertOne(document);
        }

    }

    /**
     * mapper
     */
    static class MongoDbMapper extends Mapper<LongWritable, PersonMongoDbWritable, IntWritable, PersonMongoDbWritable> {
        @Override
        protected void map(LongWritable key, PersonMongoDbWritable value, Context context)
                throws IOException, InterruptedException {
            if (value.age == null) {
                System.out.println("过滤数据" + value.name);
                return;
            }
            context.write(new IntWritable(value.age), value);
        }
    }

    /**
     * 自定义reducer
     *
     * @author gerry
     */
    static class MongoDbReducer extends Reducer<IntWritable, PersonMongoDbWritable, NullWritable, PersonMongoDbWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<PersonMongoDbWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (PersonMongoDbWritable value : values) {
                sum += value.count;
            }
            PersonMongoDbWritable personMongoDBWritable = new PersonMongoDbWritable();
            personMongoDBWritable.age = key.get();
            personMongoDBWritable.count = sum;
            context.write(NullWritable.get(), personMongoDBWritable);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 设置intputformat的value 类
        conf.setClass("mapreduce.mongo.split.value.class", PersonMongoDbWritable.class, MongoDbWritable.class);
        Job job = Job.getInstance(conf, "自定义input/outputformat");
        job.setJarByClass(MongoDbRunner.class);
        job.setMapperClass(MongoDbMapper.class);
        job.setReducerClass(MongoDbReducer.class);
        // mapper输出key
        job.setMapOutputKeyClass(IntWritable.class);
        // mapper输出value
        job.setMapOutputValueClass(PersonMongoDbWritable.class);
        // reducer输出key
        job.setOutputKeyClass(NullWritable.class);
        // reducer 输出value
        job.setOutputValueClass(PersonMongoDbWritable.class);
        // 设置intputformat
        job.setInputFormatClass(MongoDbInputFormat.class);
        // 设置otputformat
        job.setOutputFormatClass(MongoDbOutputFormat.class);
        job.waitForCompletion(true);
    }
}
