package com.genx.quotation.flinkstore.main;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.genx.quotation.flinkstore.rich.MinuteKLineRichFunction;
import com.genx.quotation.flinkstore.sink.MongoDbSink;
import com.genx.quotation.flinkstore.transform.QuotationTradeTansformer;
import com.genx.quotation.flinkstore.vo.QuotationKlineItem;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2019/1/10 10:06
 */
public class QuotationTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 非常关键，一定要设置启动检查点！！
        env.enableCheckpointing(5000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.190:9092");

        /*
        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "localhost:2181");
        */

        properties.setProperty("group.id", "test1");

        //从kafka 获取数据流
        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer("coin.quotation.trade.detail", new SimpleStringSchema(), properties));

        // 将String 转成 元组
        SingleOutputStreamOperator<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>> stream1 = stream.flatMap(new QuotationTradeTansformer());

        //根据 exchangeCode, symbol 分流
        KeyedStream<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>, Tuple> keyedStream = stream1
                .keyBy(0, 1);

        //具体计算
        SingleOutputStreamOperator<QuotationKlineItem> result = keyedStream.flatMap(new MinuteKLineRichFunction());

        //写数据
        result.addSink(new MongoDbSink());

        env.execute("QuotationTest");
    }
}
