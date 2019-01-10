package com.genx.quotation.flinkstore.sink;

import com.genx.quotation.flinkstore.vo.QuotationKlineItem;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import javax.management.Query;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;


/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2019/1/9 21:18
 */
public class MongoDbSink extends RichSinkFunction<QuotationKlineItem> {

    private transient MongoCollection collection;

    @Override
    public void invoke(QuotationKlineItem value, SinkFunction.Context context) {
        String id = value.getExchangeCode() + "_" + value.getSymbol() + "_" + value.getMinute();

        Document document = new Document("$set", new Document("exchangeCode", value.getExchangeCode())
                .append("symbol", value.getSymbol())
                .append("minute", value.getMinute())
                .append("open", value.getOpen())
                .append("close", value.getClose())
                .append("low", value.getLow())
                .append("high", value.getHigh())
                .append("amount", value.getAmount())
                .append("num", value.getNum())
                .append("vol", value.getVol()));
        UpdateOptions updateOptions = new UpdateOptions().upsert(true);
        collection.updateOne(Filters.eq("_id", id), document, updateOptions);
    }

    @Override
    public void open(Configuration config) {
        ServerAddress serverAddress = new ServerAddress("192.168.1.126", 27017);
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
        collection = mongoDatabase.getCollection("kline_minute");
    }


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

        UpdateOptions updateOptions = new UpdateOptions();

        updateOptions.upsert(true);

        String id = 10004 + "_" + "xrpusdt" + "_" + 123;

        Document document = new Document("symbol","rxpusdt")
                .append("exchangeCode", 123123);

//        collection.updateOne(Filters.eq("_id", id), document, updateOptions);

        UpdateResult updateResult = collection.updateOne(Filters.eq("_id",  id),
                new Document("$min", new Document("low", new BigDecimal("41.5")))
                        .append("$max", new Document("high", new BigDecimal("42.12")))
                        .append("$set", new Document("close", new BigDecimal("42.13")))
                ,updateOptions
        );

//        collection.insertOne(document);

    }
}
