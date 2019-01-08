package com.genx.quotation.datacenter.kafka;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Properties;

public class SimpleKafkaProducer {
    private static KafkaProducer<String, String> producer;
    private final static String TOPIC = "coin.quotation.trade.detail";

    public SimpleKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.111:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //设置分区类,根据key进行数据分区
        producer = new KafkaProducer<>(props);
    }

    public void produce() {
    Logger logger = LoggerFactory.getLogger(getClass());
        String[] symbols = new String[]{"10004-btcusdt", "10004-ethbtc", "10004-bchbtc"};

        JSONObject item;
        for (int i = 1; i < 10000; i++) {
            item = new JSONObject();
            item.put("symbol", symbols[RandomUtils.nextInt(0, symbols.length)]);
            item.put("price", new BigDecimal(Math.random()));
            item.put("amount", new BigDecimal(Math.random()));
            item.put("time", System.currentTimeMillis());
            logger.info(item.toJSONString());
            producer.send(new ProducerRecord<>(TOPIC, item.toJSONString()));

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }

    public static void main(String[] args) {
        new SimpleKafkaProducer().produce();

    }
}