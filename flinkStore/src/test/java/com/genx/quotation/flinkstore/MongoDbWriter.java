package com.genx.quotation.flinkstore;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2019/1/11 20:03
 */
public class MongoDbWriter {

    public static void main(String[] args) {
        ServerAddress serverAddress = new ServerAddress("localhost", 27017);
        List<ServerAddress> addressList = new ArrayList<ServerAddress>();
        addressList.add(serverAddress);
        // 认证信息（三个参数分别为：用户名、数据库名称、密码）
//        MongoCredential credential = MongoCredential.createScramSha1Credential("dsp", "dsp", "shi".toCharArray());
//        List<MongoCredential> credentialList = new ArrayList<MongoCredential>();
//        credentialList.add(credential);
        // 根据URI和认证信息获取数据库连接
        MongoClient mongoClient = new MongoClient(addressList);

        /** 切换数据库 */
        MongoDatabase mongoDatabase = mongoClient.getDatabase("quotation");
        /** 切换到需要操作的集合 */
        MongoCollection collection = mongoDatabase.getCollection("kline_minute");

        final int exchangeCode = 10004;
        final String symbol = "btcusd";
        final BigDecimal open = new BigDecimal("214.123");
        final BigDecimal close = new BigDecimal("214.123");
        final BigDecimal low = new BigDecimal("214.123");
        final BigDecimal high = new BigDecimal("214.123");
        final BigDecimal amount = new BigDecimal("214.123");
        final long num = 123;
        final BigDecimal vol = new BigDecimal("212314.123");

        Document document;
        List<Document> list = new ArrayList<>(10000);
        long a = System.currentTimeMillis();
        int minute = (int) (a / 60000);
        for (int i = 100000000; i < 200000000; i++) {
            try{
                document = new Document("_id", " 10004_btcusd_" + (minute + i))
                        .append("exchangeCode", exchangeCode)
                        .append("symbol", symbol)
                        .append("minute", minute + i)
                        .append("open", open)
                        .append("close", close)
                        .append("low", low)
                        .append("high", high)
                        .append("amount", amount)
                        .append("num", num)
                        .append("vol", vol);
                list.add(document);
                if (list.size() >= 10000) {
                    collection.insertMany(list, new InsertManyOptions().ordered(false));
                    list.clear();
                    System.out.println(i);
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }
        if (list.size() > 0) {
            collection.insertMany(list, new InsertManyOptions().ordered(false));
        }

        long b = System.currentTimeMillis();

        System.out.println("共耗时：" + (b - a) + "毫秒");
    }
}
