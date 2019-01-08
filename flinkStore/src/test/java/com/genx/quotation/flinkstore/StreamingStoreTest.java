package com.genx.quotation.flinkstore;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2019/1/4 23:14
 */
public class StreamingStoreTest {
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
        properties.setProperty("group.id", "test");

        //从kafka 获取数据流
        DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<>("coin.quotation.trade.detail", new SimpleStringSchema(), properties));

        // 将String 转成 JSONObject
        SingleOutputStreamOperator<JSONObject> jsonStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                collector.collect(JSON.parseObject(s));
            }
        });


        KeyedStream<JSONObject, String> keyedStream = jsonStream
                .keyBy(new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject event) {
                        return event.getString("symbol");
                    }
                });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream
                .flatMap(new RichFlatMapFunction<JSONObject, Tuple2<String, Integer>>() {
                    private transient ValueState<JSONObject> modelState;

                    @Override
                    public void flatMap(JSONObject data, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        logger.info(data.toJSONString());
                        if(modelState.value() == null){
                            data.put("count", 1);
                            modelState.update(data);
                        } else {
                            int count = modelState.value().getIntValue("count");
                            data.put("count", count + 1);
                            modelState.update(data);
                        }
                        out.collect(new Tuple2(data.getString("symbol"), modelState.value().getIntValue("count")));
                    }

                    @Override
                    public void open(Configuration config) {
                        ValueStateDescriptor<JSONObject> descriptor =
                                new ValueStateDescriptor(
                                        // state name
                                        "symbolCount",
                                        // type information of state
                                        TypeInformation.of(JSONObject.class));
                        modelState = getRuntimeContext().getState(descriptor);
                    }
                });

        result.print();


        // run the prediction pipeline
        env.execute("Taxi Ride Prediction");
    }

    public static final class LineSplitter implements FlatMapFunction<JSONObject, JSONObject> {
        private static final long serialVersionUID = 1L;

        public void flatMap(JSONObject value, Collector<JSONObject> out) {
//
            out.collect(value);

        }
    }
}
