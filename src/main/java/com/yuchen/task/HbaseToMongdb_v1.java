package com.yuchen.task;

import com.mongodb.*;
import com.yuchen.task.utils.MyDateUtils;
import org.apache.avro.data.Json;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class HbaseToMongdb_v1 {
    private static final String TABLE_NAME = "MY_TABLE_NAME_TOO";
    private static final String CF_DEFAULT = "DEFAULT_COLUMN_FAMILY";

    /** 声明一个 Connection类型的静态属性，用来缓存一个已经存在的连接对象 */
    public static Connection hbaseConnection = null;
    public static Admin admin = null;

    private static final Logger logger = LoggerFactory.getLogger(HbaseToMongdb_v1.class);

    public static Configuration  config = null;

    public HbaseToMongdb_v1() throws IOException, URISyntaxException {
        init();
    }
    void init() throws IOException, URISyntaxException {
        config = HBaseConfiguration.create();
        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        // 系统环境变量读取配置文件
//        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
//        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        // 加载本地配置文件
        config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));
        hbaseConnection = ConnectionFactory.createConnection(config);
        admin = hbaseConnection.getAdmin();
        System.setProperty("HADOOP_USER_NAME","hbase");
        System.out.println("connect hbase");
    }
    @Before
    public static void conn() throws IOException, InterruptedException, URISyntaxException {
        config = HBaseConfiguration.create();
        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        // 系统环境变量读取配置文件
//        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
//        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));

        // 加载本地配置文件
        config.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        config.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));
        hbaseConnection = ConnectionFactory.createConnection(config);
        admin = hbaseConnection.getAdmin();
        System.setProperty("HADOOP_USER_NAME","hbase");
        System.out.println("Before is done.");
    }

    public void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
            System.out.println("table Exists, delete and recreate table.");
        }
        admin.createTable(table);
        System.out.println("create table.");
    }

    /*
    * 创建hbase表
    * */
    @Test
    public void createSchemaTables() throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Algorithm.NONE));
            System.out.println("Creating table.");
            createOrOverwrite(admin, table);
            System.out.println("Done.");
        }
    }

    /*
     * 插入hbase表
     * note：put可以插入单条，也可以批量插入多条
     * */
    @Test
    public void insertTables() throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(TABLE_NAME);
            Table table = connection.getTable(tableName);
            // 单条put的主键
            Put put = new Put(Bytes.toBytes("1111111111111"));
            // 单条put的column, 插入三条数据
            put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("name"), Bytes.toBytes("hqp"));
            put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("age"), Bytes.toBytes("12"));
            put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("sex"), Bytes.toBytes("male"));
            System.out.println("inserting table");
            table.put(put);
            System.out.println("inserted");
            table.close();
        }
    }

    /**筛选 url_hbase_v1符合条件的数据，获取其URL
     * 1.gkg，gal,ngr,index都存在date_part字段且index的date_part字段是七天内数据
     *
    * */
    public List<String> query5fc(String timeBefore) throws IOException {
        List<String> rowkeylist = new ArrayList<>();
        List<String> datepartlist = new ArrayList<>();
        List<String> nonelist = new ArrayList<>();
        // 获得url_hbase_v1表
        TableName tableName = TableName.valueOf("url_hbase_v1");
        try (Table table = hbaseConnection.getTable(tableName)){
            Scan scan = new Scan();
            // 取出前10000个版本
            // scan.readVersions(10000);
            // scan.addFamily(Bytes.toBytes("geg"));
            int columnNum = 5;
            scan.addColumn(Bytes.toBytes("gkg"), Bytes.toBytes("date_part"));
            scan.addColumn(Bytes.toBytes("gal"), Bytes.toBytes("date_part"));
            scan.addColumn(Bytes.toBytes("geg"), Bytes.toBytes("date_part"));
            scan.addColumn(Bytes.toBytes("ngr"), Bytes.toBytes("date_part"));
            scan.addColumn(Bytes.toBytes("index"), Bytes.toBytes("date_part"));

            // 设置时间戳范围
            if(timeBefore == "1"){ //至十五分钟前的一小时数据
                scan.setTimeRange(MyDateUtils.subDateOneHour(), MyDateUtils.sub15Minute());
            }else if(timeBefore == "2"){ //至十五分钟前的两小时数据
                scan.setTimeRange(MyDateUtils.subDateTwoHour(), MyDateUtils.sub15Minute());
            }else if(timeBefore == "0"){ //0点至至十五分钟前
                scan.setTimeRange(MyDateUtils.getZeroTimestamp(), MyDateUtils.sub15Minute());
            }else { // 默认取before天前至现在的时间戳
                scan.setTimeRange(MyDateUtils.getBeforTimestamp(-1), MyDateUtils.sub15Minute());
            }

            // scan.setTimeRange(MyDateUtils.getZeroTimestamp(), MyDateUtils.getCurrentTimestamp());
            ResultScanner scanner = table.getScanner(scan);
            // 将结果的rowkey存入datalist
            long startTimestamp = MyDateUtils.getCurrentTimestamp();
            int testCount = 0;
            for (Result result : scanner) {
                // 主键
                String rowkey = Bytes.toString(result.getRow());
                // 5个库信息都有
                if(result.rawCells().length == columnNum){
                    // index的date_part是七天内
                    Cell cell = result.getColumnLatestCell(Bytes.toBytes("index"), Bytes.toBytes("date_part"));
                    String index_date_part = new String(CellUtil.cloneValue (cell), StandardCharsets.UTF_8);
                    String now_date_part = MyDateUtils.nowDate(new Date());
                    String seven_date_part = MyDateUtils.subDateSeven(new Date());
                    // 七天内存入
                    if(index_date_part.compareTo(seven_date_part) > 0 && index_date_part.compareTo(now_date_part) <= 0){
                        rowkeylist.add(rowkey);
                        //测试，有数据则跳出，需要注释
                        testCount++;
//                        if(testCount == totalCount){
//                            break;
//                        }
                        System.out.println(index_date_part);
                    }else { //七天外丢弃
                        nonelist.add(Bytes.toString(result.getRow()));
                        datepartlist.add(index_date_part);
                        System.out.println("七天外");
                    }
                }else{
                    nonelist.add(Bytes.toString(result.getRow()));
                    System.out.println("库信息不足");

                }
                System.out.println("\n-------------------------------------------------------------");
            }
            long endTimestamp = MyDateUtils.getCurrentTimestamp();
            System.out.println(Json.toString(rowkeylist));

            System.out.println("\n查询完毕, 用时" + (endTimestamp - startTimestamp) + "mm");
            System.out.println("rowkeylist：" + rowkeylist.size());
            System.out.println("datepartlist：" + datepartlist.size());
            System.out.println("nonelist：" + nonelist.size());
            scanner.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return  rowkeylist;
    }

    /**筛选 event_hbase_v1近期数据
     * timmeBefore:
     *  00:00表示0点至现在
     *  01:00表示一小时前
     *  02:00表示2小时前
     *  -1表示一天前
     * */
    public HashMap<String, DBObject> scanEnvenForLatest(String timeBefore) throws IOException {
        TableName tableName = TableName.valueOf("event_hbase_v1");
        List<String> rowkeylist = new ArrayList<>();
        List<String> datepartlist = new ArrayList<>();
        List<String> nonelist = new ArrayList<>();
        // 生成map形式，key用于消息推送，value用于插入mango
        HashMap<String, DBObject> resultMap = new HashMap<>();
        // 直接生成插入mangdb的对象
        List<DBObject> resultList = new ArrayList<DBObject>();
        try (Table table = hbaseConnection.getTable(tableName);){
            Scan scan = new Scan();
            // 设置时间戳范围
            if(timeBefore == "1"){ //至十五分钟前的一小时数据
                scan.setTimeRange(MyDateUtils.subDateOneHour(), MyDateUtils.sub15Minute());
            }else if(timeBefore == "2"){ //至十五分钟前的两小时数据
                scan.setTimeRange(MyDateUtils.subDateTwoHour(), MyDateUtils.sub15Minute());
            }else if(timeBefore == "0"){ //0点至至十五分钟前
                scan.setTimeRange(MyDateUtils.getZeroTimestamp(), MyDateUtils.sub15Minute());
            }else { // 默认取before天前至现在的时间戳
                scan.setTimeRange(MyDateUtils.getBeforTimestamp(-1), MyDateUtils.sub15Minute());
            }

            ResultScanner scanner = table.getScanner(scan);
            // 将结果的rowkey存入datalist
            long startTimestamp = MyDateUtils.getCurrentTimestamp();
            // test
            //int testCount = 0;
            for (Result result : scanner) {
                // 主键
                String rowkey = Bytes.toString(result.getRow());
                DBObject resultDBObject = new BasicDBObject();
                resultDBObject.put("_id", rowkey);
                // 1个库信息都有
                if(result.rawCells().length >= 1){
                    // exp的date_part是三天内则存入
                    Cell cell01 = result.getColumnLatestCell(Bytes.toBytes("exp"), Bytes.toBytes("date_part"));
                    String index_date_part = new String(CellUtil.cloneValue (cell01), StandardCharsets.UTF_8);
                    String now_date_part = MyDateUtils.nowDate(new Date());
                    String three_date_part = MyDateUtils.subDateThree(new Date());
                    // 三天内存入
                    if(index_date_part.compareTo(three_date_part) > 0 && index_date_part.compareTo(now_date_part) <= 0){
                        rowkeylist.add(rowkey);
                        // 遍历加入map中
                        // 获取对应的map
                        for (Cell cell : result.rawCells()) {
                            String family = new String(CellUtil.cloneFamily(cell));
                            String qualifier = new String(CellUtil.cloneQualifier(cell));
                            String key = family + '_' + qualifier.replace("\n","");
                            String value = Bytes.toString(CellUtil.cloneValue(cell));
                            // 添加到文档对象
                            resultDBObject.put(key, value);
                        }
                        resultMap.put(rowkey, resultDBObject);
                        // resultList.add(resultDBObject);
                        System.out.println(index_date_part);
                    }else { //七天外丢弃
                        nonelist.add(Bytes.toString(result.getRow()));
                        datepartlist.add(index_date_part);
                        System.out.println("三天外");
                    }
                }else{
                    nonelist.add(Bytes.toString(result.getRow()));
                    System.out.println("库信息不足");
                }
                System.out.println("\n-------------------------------------------------------------");
            }
            long endTimestamp = MyDateUtils.getCurrentTimestamp();
            System.out.println(Json.toString(rowkeylist));
            System.out.println("\n查询完毕, 用时" + (endTimestamp - startTimestamp) + "mm");
            System.out.println("rowkeylist：" + rowkeylist.size());
            System.out.println("datepartlist：" + datepartlist.size());
            System.out.println("nonelist：" + nonelist.size());
            scanner.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return  resultMap;
    }

    /**mention表或者url中get查询一批URL
     *1.URL对应mention列表
     *2.mention对应属性列表
     * {URL: { mentionID : {mention_qualifier, value}, mentionID : { mention_qualifier, value},...} }
     * */
    private HashMap<String, DBObject> urlGetBatch(List<String> rowkeyList) throws IOException {
        String tableName = "url_hbase_v1";
        Table table = hbaseConnection.getTable( TableName.valueOf(tableName));// 获取表

        // 批量查询对应的mentions
        ArrayList getList = new ArrayList();
        // {URL: { mentionID : {mention_qualifier, value}, mentionID : { mention_qualifier, value}...
        // 返回结果
        HashMap<String, DBObject> resultMap = new HashMap<>();


        for (String rowkey : rowkeyList){//把rowkey加到get里，再把get装到list中
            Get get = new Get(Bytes.toBytes(rowkey));
            getList.add(get);
        }
        // 开始时间
        long startTimestamp_get = MyDateUtils.getCurrentTimestamp();
        Result[] results = table.get(getList);//重点在这，直接查getList<Get>
        // 结束时间
        long endTimestamp_get = MyDateUtils.getCurrentTimestamp();
        System.out.println("Timestamp_get:" + (endTimestamp_get - startTimestamp_get));
        //对返回的结果集进行操作
        for (Result result : results){
            // 获取主键以及对应的map
            String rowkey = Bytes.toString(result.getRow());
            // mangodb存储对象
            DBObject resultDBObject = new BasicDBObject();
            // 如果是url_hbase_v1则存主键，否则不存主键
            resultDBObject.put("_id", rowkey);
            for (Cell cell : result.rawCells()) {
                String family = new String(CellUtil.cloneFamily(cell));
                String qualifier = new String(CellUtil.cloneQualifier(cell));
                String key = family + '_' + qualifier.replace("\n","");
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                resultDBObject.put(key, value);
            }
            resultMap.put(rowkey, resultDBObject);

            //测试，有数据则跳出，需要注释
//            if(!resultList.isEmpty()){
//                break;
//            }
        }
        long endTimestamp = MyDateUtils.getCurrentTimestamp();
        System.out.println("批量get耗时:" + (endTimestamp - startTimestamp_get));
        return resultMap;
    }
    private HashMap<String, List<DBObject>> mentionGetBatch(List<String> rowkeyList) throws IOException {
        String tableName = "urlmention_hbase_v1";
        Table table = hbaseConnection.getTable( TableName.valueOf(tableName));// 获取表

        // 批量查询对应的mentions
        ArrayList getList = new ArrayList();
        // {URL: { mentionID : {mention_qualifier, value}, mentionID : { mention_qualifier, value}...
        // 返回结果
        HashMap<String, List<DBObject>> resultMap = new HashMap<>();

        for (String rowkey : rowkeyList){//把rowkey加到get里，再把get装到list中
            Get get = new Get(Bytes.toBytes(rowkey));
            getList.add(get);
        }
        // 开始时间
        long startTimestamp_get = MyDateUtils.getCurrentTimestamp();
        Result[] results = table.get(getList);//重点在这，直接查getList<Get>
        // 结束时间
        long endTimestamp_get = MyDateUtils.getCurrentTimestamp();
        System.out.println("startTimestamp02:" + (endTimestamp_get - startTimestamp_get));
        //对返回的结果集进行操作
        for (Result result : results){
            // rowkey的第一部分为url，是map的键，rowkey的第二部分用于判断放入同一列表
            String url = Bytes.toString(result.getRow()).split("_")[0];
            String mentionId = Bytes.toString(result.getRow()).split("_")[1];
            // 取出对应list，新添加一个mention
            List<DBObject> mentionList = resultMap.getOrDefault(url, new ArrayList<>());
            DBObject resultDBObject = new BasicDBObject();
            for (Cell cell : result.rawCells()) {
                String family = new String(CellUtil.cloneFamily(cell));
                String qualifier = new String(CellUtil.cloneQualifier(cell));
                String key = family + '_' + qualifier.replace("\n","");
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                resultDBObject.put(key, value);
            }
            mentionList.add(resultDBObject);
            resultMap.put(url, mentionList);

            //测试，有数据则跳出，需要注释
//            if(!resultList.isEmpty()){
//                break;
//            }
        }
        long endTimestamp = MyDateUtils.getCurrentTimestamp();
        System.out.println("批量get耗时:" + (endTimestamp - startTimestamp_get));
        return resultMap;
    }
    /**mention表中get查询一批URL
     *1.URL对应mention列表
     *2.mention对应属性列表
     * {URL: { mentionID : {mention_qualifier, value}, mentionID : { mention_qualifier, value},...} }
     * */
    public List<String> queryMention(List<String> rowkeyList) throws IOException {
        String tableName = "urlmention_hbase_v1";
        // 结果存入newRowkeylist
        List<String> newRowkeylist = new ArrayList<>();

        try (Table table = hbaseConnection.getTable(TableName.valueOf(tableName))) {
            // 日志变量
            int testsize = rowkeyList.size();
            int testCount = 0;
            // 晒出10个mention就返回插入mangdb
            int count_sucess = 0;
            int count_failed = 0;
            // 开始时间
            long startTimestamp = MyDateUtils.getCurrentTimestamp();
            for (String url : rowkeyList) {
                long startTimestamp01 = MyDateUtils.getCurrentTimestamp();
                //总查询次数
                testCount++;
                // 前缀过滤器, rowkey作为前缀
                Scan scan = new Scan();
                PrefixFilter filter = new PrefixFilter(Bytes.toBytes(url));
                scan.setFilter(filter);
                long startTimestamp02 = MyDateUtils.getCurrentTimestamp();
                System.out.println("startTimestamp02:" + (startTimestamp02 - startTimestamp01));
                long startTimestamp_scan = MyDateUtils.getCurrentTimestamp();
                ResultScanner results = table.getScanner(scan);
                long endTimestamp_scan = MyDateUtils.getCurrentTimestamp();
                System.out.println("scan用时:" + (endTimestamp_scan - startTimestamp_scan) + "mm");
                long startTimestamp03 = MyDateUtils.getCurrentTimestamp();
                System.out.println("startTimestamp03:" + (startTimestamp03 - startTimestamp02));
                // 辅助变量c==0则表示没有对应mention
                int c = 0;//辅助变量
                for (Result result : results){
                    // 辅助变量
                    c++;
                    // 存主键
                    String rowkey = Bytes.toString(result.getRow());
                    newRowkeylist.add(rowkey);
                    // 测试
                }
                long startTimestamp04 = MyDateUtils.getCurrentTimestamp();
                System.out.println("startTimestamp04:" + (startTimestamp04 - startTimestamp03));

                if(c==0){
                    // 打印日志
                    count_failed++;
                    System.out.println("mention未查询到,失败数：" + count_failed);
                    System.out.println("已查询数：" + testCount);
                    System.out.println("总需要查询数：" + testsize);
                }else{
                    count_sucess++;


                    // 打印日志
                    System.out.println("mention查询到,成功数：" + count_sucess);
                    System.out.println("总查询数：" + testCount);
                    System.out.println("总需要查询数：" + testsize);
                    // 测试代码, 只返回2条, 需要注释
                    if(count_sucess == 10){
                        return newRowkeylist;
                    }
                }
                System.out.println("--------------------------------------");
            }
            long endTimestamp = MyDateUtils.getCurrentTimestamp();
            System.out.println("mention查询完毕, 用时" + (endTimestamp - startTimestamp) + "mm");
            System.out.println("总查询条数：" + testCount);
            System.out.println("成功条数：" + count_sucess);
            System.out.println("失败条数：" + count_failed);
        } catch (Exception ignored) {
        }
        return newRowkeylist;
    }

    public DBCollection getMongdbCollection(String host, int port, String dbname, String collname) throws IOException {
        MongoClient mongoClient = new MongoClient(host, port);
        DB db = mongoClient.getDB(dbname);
        return db.getCollection(collname);
    }

    /** 定期将hbase数据更新到mongodb
     * TODO：eventMessageList推送功能
     * */
    public void hbase2mangoEventTable(DBCollection eventCollection) throws IOException, URISyntaxException, InterruptedException {
        // hbase
        // eventMap
        HashMap<String, DBObject> eventMap = this.scanEnvenForLatest("-2");

        // mangodb
        // 操作mongo
        // 遍历集合取eventMessageList用于推送, eventList用于存入mangodb
        List<String> eventMessageList = new ArrayList<>();
        List<DBObject> eventList = new ArrayList<DBObject>();
        eventMap.forEach((key,value)->{
            eventMessageList.add(key);
            eventList.add(value);
        });
        // 批量写入
        // eventCollection.insert(eventList);
        // 无法批量覆写，只能是遍历save覆写单条,存入mango
        System.out.println("正在存入mango");
        eventList.forEach(eventCollection::save);
        System.out.println("mango存入完毕");
        // 打印最终的推送列表
        System.out.println(eventMessageList);
    }

    /** 筛选拼接hbase表，存入mangodb
     * TODO：urlMessageList推送功能
     * */
    public void hbase2mangoUrlTable(DBCollection urlCollection) throws IOException, URISyntaxException, InterruptedException {
        // hbase
        // 操作hbase
        // 筛选出geg,gkg,gal,ngr,index都有的rowkey
        List<String> query5fcResults = this.query5fc("2");
        // 拿到对应rowkey的所有url_hbase_v1数据, 主键为URL
        HashMap<String, DBObject> urlMap = this.urlGetBatch(query5fcResults); // DBObject包含主键 _id
        // 拿到url对应的所有mention list, 返回所有符合条件的主键组成的list，这里测试限制为10条
        List<String>  queryMentionResults = this.queryMention(query5fcResults);
        // 根据mention list主键列表批量查询
        HashMap<String, List<DBObject>> url2mentionMap = this.mentionGetBatch(queryMentionResults);// DBObject中不包含主键 _id

        // 空间换时间：map的O(1)操作合并相同主键的信息，将url_mention中的list合并到url表中，一次性存入mongdb
        // urlMessageList为推送列表，urlList为mongo存储列表
        List<String> urlMessageList = new ArrayList<>();
        List<DBObject> urlList = new ArrayList<>();
        url2mentionMap.forEach((key,value)->{
            System.out.println("key = " + key);
            System.out.println("value = " + value);
            DBObject resultDBObject = urlMap.getOrDefault(key, new BasicDBObject());
            resultDBObject.put("mentionList", value);
            urlMessageList.add(key);
            urlList.add(resultDBObject);
        });
        System.out.println("**********************************************");

        // 批量写入
        // urlCollection.insert(urlList);
        // 无法批量覆写，只能是遍历save覆写单条,存入mango
        System.out.println("正在存入mango");
        urlList.forEach(urlCollection::save);
        System.out.println("mango存入完毕");
        // 打印最终的推送列表
        System.out.println(urlMessageList);
    }

    public void close() throws IOException {
        try{
            if(hbaseConnection != null){
                hbaseConnection.close();
            }
            if(admin != null){
                admin.close();
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        HbaseToMongdb_v1 hbaseToMongdb = new HbaseToMongdb_v1();
        // mongodb连接
        DBCollection eventCollection;
        DBCollection urlCollection;
        try {
            eventCollection = hbaseToMongdb.getMongdbCollection("192.168.12.180", 28100, "gdelt", "event");
            urlCollection = hbaseToMongdb.getMongdbCollection("192.168.12.180", 28100, "gdelt", "url");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // 事件表
        // hbaseToMongdb.hbase2mangoEventTable(eventCollection);

        // url表
        hbaseToMongdb.hbase2mangoUrlTable(urlCollection);
        // 释放资源
        hbaseToMongdb.close();
    }
}