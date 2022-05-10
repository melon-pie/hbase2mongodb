package com.yuchen.task;

import com.alibaba.fastjson.JSON;
import org.bson.Document;
import com.mongodb.*;
import com.yuchen.task.utils.MyDateUtils;
import org.apache.avro.data.Json;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import  java.util.Calendar;
import  java.util.Date;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class HbaseToMongdb {
    private static final String TABLE_NAME = "MY_TABLE_NAME_TOO";
    private static final String CF_DEFAULT = "DEFAULT_COLUMN_FAMILY";

    /** 声明一个 Connection类型的静态属性，用来缓存一个已经存在的连接对象 */
    private static Connection hbaseConnection = null;

    private static final Logger logger = LoggerFactory.getLogger(HbaseToMongdb.class);
    private static Admin admin = null;

    public static Configuration  config = null;

    public static int  totalCount = 1;

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

    /*
     * get查询hbase表，
     * note：put可以插入单条，也可以批量插入多条
     * */
    @Test
    public void getQuery() throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(TABLE_NAME);
            Table table = connection.getTable(tableName);

            byte[] rowkey = Bytes.toBytes("1111111111111");
            System.out.println("getQuery");
            Get get = new Get(rowkey);
            // 在服务段做数据过滤，挑选出符合需求的列，不添加的话默认取出一整行数据，如果列很多的话，效率很低
            get.addFamily(Bytes.toBytes(CF_DEFAULT));
            get.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("name"));
            get.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("age"));
            get.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("sex"));

            Result result = table.get(get);
            Cell cell01 = result.getColumnLatestCell(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("name"));
            Cell cell02 = result.getColumnLatestCell(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("age"));
            Cell cell03 = result.getColumnLatestCell(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("sex"));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell01)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell02)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell03)));
            table.close();
        }
    }

    /*
     * scan查询hbase表
     * */

    public static long getTwoHourBeforeTimestamp(){
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
    }


    /**筛选 url_hbase_v1符合条件的数据，获取其URL
     * 1.gkg，gal,ngr,index都存在date_part字段且index的date_part字段是七天内数据
     *
    * */
    @Test
    public static List<String> query5fc(String timeBefore) throws IOException {
        Table table = null;
        List<String> rowkeylist = new ArrayList<>();
        List<String> datepartlist = new ArrayList<>();
        List<String> nonelist = new ArrayList<>();

        try {
            // 全部查询结束后再close
            // 获得url_hbase_v1表
            TableName tableName = TableName.valueOf("url_hbase_v1");
            table = hbaseConnection.getTable(tableName);
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
            if(timeBefore == "01:00"){ //一小时前至现在
                scan.setTimeRange(MyDateUtils.subDateOneHour(), MyDateUtils.getCurrentTimestamp());
            }else if(timeBefore == "02:00"){ //两小时前至现在
                scan.setTimeRange(MyDateUtils.subDateTwoHour(), MyDateUtils.getCurrentTimestamp());
            }else if(timeBefore == "00:00"){ //0点至现在的时间戳
                scan.setTimeRange(MyDateUtils.getZeroTimestamp(), MyDateUtils.getCurrentTimestamp());
            }else { // 默认取before天前至现在的时间戳
                scan.setTimeRange(MyDateUtils.getBeforTimestamp(-1), MyDateUtils.getCurrentTimestamp());
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
        }finally {
            if(table != null){
                table.close();
            }
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
    public static List<DBObject> scanEnvenForLatest(String timeBefore) throws IOException {
        Table table = null;
        List<String> rowkeylist = new ArrayList<>();
        List<String> datepartlist = new ArrayList<>();
        List<String> nonelist = new ArrayList<>();
        List<DBObject> resultList = new ArrayList<DBObject>();
        try {
            // 全部查询结束后再close
            // 获得url_hbase_v1表
            TableName tableName = TableName.valueOf("event_hbase_v1");
            table = hbaseConnection.getTable(tableName);
            Scan scan = new Scan();

            // 设置时间戳范围
            if(timeBefore == "01:00"){ //一小时前至现在
                scan.setTimeRange(MyDateUtils.subDateOneHour(), MyDateUtils.getCurrentTimestamp());
            }else if(timeBefore == "02:00"){ //两小时前至现在
                scan.setTimeRange(MyDateUtils.subDateTwoHour(), MyDateUtils.getCurrentTimestamp());
            }else if(timeBefore == "00:00"){ //0点至现在的时间戳
                scan.setTimeRange(MyDateUtils.getZeroTimestamp(), MyDateUtils.getCurrentTimestamp());
            }else { // 默认before天前至现在的时间戳
                scan.setTimeRange(MyDateUtils.getBeforTimestamp(-1), MyDateUtils.getCurrentTimestamp());
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
                        resultList.add(resultDBObject);
                        // 增加到resultsMap
                        //测试，有数据则跳出，需要注释
//                        if(!resultsMap.isEmpty()){
//                            break;
//                        }
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
        }finally {
            if(table != null){
                table.close();
            }
        }
        return  resultList;
    }

    /**mention表中get查询一批URL
     *1.URL对应mention列表
     *2.mention对应属性列表
     * {URL: { mentionID : {mention_qualifier, value}, mentionID : { mention_qualifier, value},...} }
     * */
    private static List<DBObject> urlGetBatch(List<String> rowkeyList) throws IOException {
        String tableName = "url_hbase_v1";
        Table table = hbaseConnection.getTable( TableName.valueOf(tableName));// 获取表

        // 批量查询对应的mentions
        ArrayList getList = new ArrayList();
        // {URL: { mentionID : {mention_qualifier, value}, mentionID : { mention_qualifier, value}...

        // 返回结果
        List<DBObject> resultList = new ArrayList<DBObject>();
        // Map<String,  Map<String, ArrayList<String>>> resultsMap = new HashMap<>();


        for (String rowkey : rowkeyList){//把rowkey加到get里，再把get装到list中
            Get get = new Get(Bytes.toBytes(rowkey));
            getList.add(get);
        }
        // 开始时间
        long startTimestamp_get = MyDateUtils.getCurrentTimestamp();
        Result[] results = table.get(getList);//重点在这，直接查getList<Get>
        // 结束时间
        long endTimestamp_get = MyDateUtils.getCurrentTimestamp();

        //对返回的结果集进行操作
        for (Result result : results){
            // 获取主键以及对应的map
            String rowkey = Bytes.toString(result.getRow());
            DBObject resultDBObject = new BasicDBObject();
            resultDBObject.put("_id", rowkey);
            
            for (Cell cell : result.rawCells()) {
                String family = new String(CellUtil.cloneFamily(cell));
                String qualifier = new String(CellUtil.cloneQualifier(cell));
                String key = family + '_' + qualifier.replace("\n","");
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                resultDBObject.put(key, value);
            }
            resultList.add(resultDBObject);
            //测试，有数据则跳出，需要注释
//            if(!resultList.isEmpty()){
//                break;
//            }
        }
        return resultList;
    }
    /**mention表中get查询一批URL
     *1.URL对应mention列表
     *2.mention对应属性列表
     * {URL: { mentionID : {mention_qualifier, value}, mentionID : { mention_qualifier, value},...} }
     * */
    public static List<DBObject> mentionGetBatch(List<String> rowkeyList) throws IOException {
        String tableName = "urlmention_hbase_v1";
        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));// 获取表

        // 批量查询对应的mentions
        ArrayList getList = new ArrayList();
        // String url = "00000024c0c9f3f1150ee303ffce51bf";
        List<String> rowkeyList1 = new ArrayList();
        // rowkeyList.add("00000024c0c9f3f1150ee303ffce51bf");

        // 返回结果
        List<DBObject> resultList = new ArrayList<DBObject>();

        int testCount = 0;
        int count_sucess = 0;
        int count_failed = 0;
        // 开始时间
        long startTimestamp = MyDateUtils.getCurrentTimestamp();
        for (String url : rowkeyList) {
            testCount++;
            Scan scan = new Scan();
            // and操作
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            // 前缀过滤器, rowkey作为前缀
            PrefixFilter filter = new PrefixFilter(Bytes.toBytes(url));
            filterList.addFilter(filter);
            scan.setFilter(filterList);
            long startTimestamp_scan = MyDateUtils.getCurrentTimestamp();
            ResultScanner results = table.getScanner(scan);
            long endTimestamp_scan = MyDateUtils.getCurrentTimestamp();
            System.out.println("scan用时:" + (startTimestamp_scan - endTimestamp_scan) + "mm");
            //对返回的结果集进行操作
            Result result = results.next();
            if(result == null){
                count_failed++;
                System.out.println("mention未查询到,失败数：" + count_failed);
                continue;
            }
            count_sucess++;
            System.out.println("mention查询到,成功数：" + count_sucess);
            String rowkey = Bytes.toString(result.getRow()).split("_")[0];
            String mentionId = Bytes.toString(result.getRow()).split("_")[1];
            DBObject resultDBObject = new BasicDBObject();
            List<DBObject> mentionList = new ArrayList<DBObject>();

            // 多个mention循环
            while (result != null){
                DBObject mentionItem = new BasicDBObject();
                // 一个mention的字段循环
                for (Cell cell : result.rawCells()) {
                    String qualifier = new String(CellUtil.cloneQualifier(cell));
                    String key = "mention_" + qualifier.replace("\n","");
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    mentionItem.put(key, value);
                }
                mentionList.add(mentionItem);
                result = results.next();
            }

            // 加入resultList
            resultDBObject.put("_id", rowkey);
            resultDBObject.put("mentionList", mentionList);
            resultList.add(resultDBObject);

            //测试，有数据则跳出，需要注释
//            if (!resultList.isEmpty()) {
//                System.out.println("查询到一条,已查询数：" + testCount);
//                break;
//            }
        }
        // 结束时间
        table.close();
        long endTimestamp = MyDateUtils.getCurrentTimestamp();
        System.out.println("mention查询完毕, 用时" + (endTimestamp - startTimestamp) + "mm");
        System.out.println("成功条数：" + count_sucess);
        System.out.println("失败条数：" + count_failed);
        return resultList;
    }

    public DBCollection getMongdbCollection(String host, int port, String dbname, String collname) throws IOException {
        MongoClient mongoClient = new MongoClient(host, port);
        DB db = mongoClient.getDB(dbname);
        return db.getCollection(collname);
    }


    public static void close() throws IOException {
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

    public static void add(String name, int age, String sex) {
        DBObject userInfo = new BasicDBObject();
        userInfo.put("name", name);
        userInfo.put("age", age);
        userInfo.put("sex", sex);


    }
//    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
//        HbaseToMongdb hbaseToMongdb = new HbaseToMongdb();
//        DBCollection collection = null;
//
//        // 连接
//        conn();
//        // 操作hbase
//        // 筛选出geg,gkg,gal,ngr,index都有的rowkey
//        List<String> query5fc_results = query5fc("02:00");
//        // 拿到对应rowkey的所有url_hbase_v1数据, 主键为URL
//         List<DBObject> urllist = urlGetBatch(query5fc_results);
//        // 根据rowkey拿到url对应的所有mention list, 主键为URL
//        List<DBObject>  mention_results = mentionGetBatch(query5fc_results);
//        close();
//
//
//        // 操作mongo
//        DBCollection urlCollection;
//        DBCollection eventCollection;
//        DBCollection urlMentionCollection;
//        try {
//            urlCollection = hbaseToMongdb.getMongdbCollection("192.168.12.180", 28100, "gdelt", "url01");
//            urlMentionCollection = hbaseToMongdb.getMongdbCollection("192.168.12.180", 28100, "gdelt", "url_mention");
//            eventCollection = hbaseToMongdb.getMongdbCollection("192.168.12.180", 28100, "gdelt", "event");
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//        //urlCollection.insert(urllist);
//        urlMentionCollection.insert(mention_results);
//        //eventCollection.insert(all_event_results);
//
//
//        if (urlCollection != null){
//            DBCursor cursor = urlCollection.find();
//            try {
//                while(cursor.hasNext()) {
//                    System.out.println(cursor.next());
//                }
//            } finally {
//                cursor.close();
//            }
//        }else {
//            System.out.println("连接失败");
//        }
//    }

}