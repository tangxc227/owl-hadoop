package com.tangxc.hadoop.mr.mongo;

import com.mongodb.DBCollection;
import com.mongodb.client.MongoCollection;
import org.apache.hadoop.io.Writable;
import org.bson.Document;

/**
 * @author Xicheng.Tang
 */
public interface MongoDbWritable extends Writable {

    /**
     * 从mongodb中读取数据
     *
     * @param document
     */
    void readFields(Document document);

    /**
     * 往mongodb中写入数据
     *
     * @param collection
     */
    void write(MongoCollection<Document> collection);

}
