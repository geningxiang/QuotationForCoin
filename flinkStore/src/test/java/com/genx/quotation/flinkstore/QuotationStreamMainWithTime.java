package com.genx.quotation.flinkstore;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.genx.quotation.flinkstore.sink.MongoDbSink;
import com.genx.quotation.flinkstore.vo.QuotationKlineItem;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2019/1/8 22:30
 */
public class QuotationStreamMainWithTime {
    public static void main(String[] args) throws Exception {
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


        DataStream<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>> withTimestampsAndWatermarks = keyedStream
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>>(){

                    @Override
                    public long extractTimestamp(Tuple5<Integer, String, Long, BigDecimal, BigDecimal> o, long l) {
                        return 0;
                    }

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return null;
                    }
                });

        DataStream<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>> withTimestampsAndWatermarks1 = keyedStream
                .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>>(){

                    @Override
                    public long extractTimestamp(Tuple5<Integer, String, Long, BigDecimal, BigDecimal> integerStringLongBigDecimalBigDecimalTuple5, long l) {
                        return 0;
                    }

                    @Nullable
                    @Override
                    public Watermark checkAndGetNextWatermark(Tuple5<Integer, String, Long, BigDecimal, BigDecimal> integerStringLongBigDecimalBigDecimalTuple5, long l) {
                        return null;
                    }
                });

//        keyedStream.timeWindow()

        //扩展WindowAssigner类来实现自定义窗口分配器
        keyedStream.window(new WindowAssigner<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>, TimeWindow>(){

            @Override
            public Collection<TimeWindow> assignWindows(Tuple5<Integer, String, Long, BigDecimal, BigDecimal> o, long l, WindowAssignerContext windowAssignerContext) {
                return null;
            }

            @Override
            public Trigger<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
                return null;
            }

            @Override
            public TypeSerializer getWindowSerializer(ExecutionConfig executionConfig) {
                return null;
            }

            @Override
            public boolean isEventTime() {
                return true;
            }
        });

        // run the prediction pipeline
        env.execute("QuotationStreamMain");
    }
}
