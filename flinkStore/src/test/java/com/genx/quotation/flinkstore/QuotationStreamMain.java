package com.genx.quotation.flinkstore;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.genx.quotation.flinkstore.sink.MongoDbSink;
import com.genx.quotation.flinkstore.vo.QuotationKlineItem;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2019/1/8 22:30
 */
public class QuotationStreamMain {
    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(StreamingStoreTest.class);

        logger.info("start");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        env.enableCheckpointing(5000); // 非常关键，一定要设置启动检查点！！

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.111:9092");
        // only required for Kafka 0.8
        //properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test1");

        //从kafka 获取数据流
        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<>("coin.quotation.trade.detail", new SimpleStringSchema(), properties));

        // 将String 转成 元组
        SingleOutputStreamOperator<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>> stream1 = stream.flatMap(new FlatMapFunction<String, Tuple5<Integer, String, Long, BigDecimal, BigDecimal>>() {
            @Override
            public void flatMap(String s, Collector<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>> collector) throws Exception {
                JSONObject json = JSON.parseObject(s);
                int exchangeCode = json.getIntValue("ec");
                String symbol = json.getString("symbol");
                JSONArray data = json.getJSONArray("data");
                JSONObject item;
                for (int i = 0; i < data.size(); i++) {
                    item = data.getJSONObject(i);
                    collector.collect(new Tuple5(exchangeCode, symbol, item.getLongValue("t"), item.getBigDecimal("p"), item.getBigDecimal("a")));
                }
            }
        });

        KeyedStream<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>, Tuple> keyedStream = stream1
                .keyBy(0, 1);


        SingleOutputStreamOperator<QuotationKlineItem> result = keyedStream
                .flatMap(new RichFlatMapFunction<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>, QuotationKlineItem>() {

                    private transient ValueState<QuotationKlineItem> modelState;

                    @Override
                    public void flatMap(Tuple5<Integer, String, Long, BigDecimal, BigDecimal> data, Collector<QuotationKlineItem> out) throws IOException {
                        if (modelState.value() == null) {
                            modelState.update(new QuotationKlineItem(data.f0, data.f1, data.f2, data.f3, data.f4));
                        } else {
                            int minute = (int) (data.f2 / 60000);
                            if (minute < modelState.value().getMinute()) {

                            } else if (minute == modelState.value().getMinute()) {
                                //分钟内的更新
                                modelState.value().update(data.f3, data.f4);
                            } else {
                                out.collect(modelState.value());

                                modelState.update(new QuotationKlineItem(data.f0, data.f1, data.f2, data.f3, data.f4));
                            }
                        }
                    }
                    @Override
                    public void open(Configuration config) {
                        ValueStateDescriptor<QuotationKlineItem> descriptor =
                                new ValueStateDescriptor(
                                        "quotationKline",
                                        TypeInformation.of(QuotationKlineItem.class));
                        modelState = getRuntimeContext().getState(descriptor);
                    }
                });

        result.addSink(new MongoDbSink());

        // run the prediction pipeline
        env.execute("QuotationStreamMain");
    }
}
