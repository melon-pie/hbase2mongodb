package com.yuchen.task.utils;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

public class monddbUtils {
    private static MongoClient mongoClient = null;

    public static void main(String args[]) {
        testQuery();
    }

    public static void testQuery() {
        MongoDatabase mongoDatabase = getDB();
        // 这里的 "user" 表示集合的名字，如果指定的集合不存在，mongoDB将会在你第一次插入文档时创建集合。
        MongoCollection<Document> collection = mongoDatabase.getCollection("user");

        // 查找集合中的所有文档
        FindIterable<Document> findIterable = collection.find();
        MongoCursor<Document> cursor = findIterable.iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

        // 指定查询过滤器
        Bson filter = Filters.eq("name", "张1");
        // 指定查询过滤器查询
        findIterable = collection.find(filter);
        cursor = findIterable.iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

        // 查找集合中的所有文档，－1表示倒序
        findIterable = collection.find().sort(new BasicDBObject("age", -1));
        // 取出查询到的第一个文档
        Document document = (Document) findIterable.first();
        // 打印输出
        System.out.println(document);

        closeDB();
    }

    public static void testUpdate() {
        MongoDatabase mongoDatabase = getDB();
        // 这里的 "user" 表示集合的名字，如果指定的集合不存在，mongoDB将会在你第一次插入文档时创建集合。
        MongoCollection<Document> collection = mongoDatabase.getCollection("user");

        // 修改过滤器
        Bson filter = Filters.eq("name", "阿娇");
        // 指定修改的更新文档
        Document document = new Document("$set", new Document("age", 22));
        // 修改单个文档
        collection.updateOne(filter, document);
        // 修改多个文档
        collection.updateMany(filter, document);

        closeDB();
    }

    public static void testDel() {
        MongoDatabase mongoDatabase = getDB();
        // 这里的 "user" 表示集合的名字，如果指定的集合不存在，mongoDB将会在你第一次插入文档时创建集合。
        MongoCollection<Document> collection = mongoDatabase.getCollection("user");

        // 申明删除条件，eq等于，也可以设置大于小于等条件
        Bson filter = Filters.eq("age", 18);
        // 删除与筛选器匹配的单个文档
        collection.deleteOne(filter);
        // 删除与筛选器匹配的所有文档
        collection.deleteMany(filter);

        closeDB();
    }

    public static void testAdd() {
        MongoDatabase mongoDatabase = getDB();
        // 这里的 "user" 表示集合的名字，如果指定的集合不存在，mongoDB将会在你第一次插入文档时创建集合。
        MongoCollection<Document> collection = mongoDatabase.getCollection("user");

        Document document = new Document("name", "阿娇").append("sex", "女").append("age", 18);
        collection.insertOne(document);

        // 要插入的数据
        List<Document> list = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            Document documents = new Document("name", "张" + i).append("sex", "男").append("age", 20 + i);
            list.add(documents);
        }
        // 插入多个文档
        collection.insertMany(list);

        closeDB();
    }

    public static MongoDatabase getDB() {
        List<ServerAddress> adds = new ArrayList<>();
        ServerAddress serverAddress = new ServerAddress("地址", 3717);
        adds.add(serverAddress);
        List<MongoCredential> credentials = new ArrayList<>();
        // 用户名 数据库名称 密码
        MongoCredential mongoCredential = MongoCredential.createScramSha1Credential("用户名", "admin",
                "密码".toCharArray());
        credentials.add(mongoCredential);
        mongoClient = new MongoClient(adds, credentials);
        // 这里的 "test" 表示数据库名，若指定的数据库不存在，mongoDB将会在你第一次插入文档时创建数据库。
        MongoDatabase mongoDatabase = mongoClient.getDatabase("test");
        return mongoDatabase;
    }

    public static void closeDB() {
        mongoClient.close();
    }
}
