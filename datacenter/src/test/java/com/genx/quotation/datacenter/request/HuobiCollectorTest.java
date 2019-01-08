package com.genx.quotation.datacenter.request;


import com.alibaba.fastjson.JSONObject;
import com.genx.quotation.collector.handler.IQuotationSink;
import com.genx.quotation.collector.handler.QuotationSinkFactory;
import com.genx.quotation.collector.msg.QuotationMsg;
import com.genx.quotation.collector.msg.TradeDetailMsg;
import com.genx.quotation.collector.request.ICaimaoSocketListener;
import com.genx.quotation.collector.request.WebSocketHandler;
import com.genx.quotation.collector.socket.SocketEventType;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *  以 engine js 的方式 订阅 火币的行情数据
 * @author: genx
 * @date: 2019/1/7 22:18
 */
public class HuobiCollectorTest {

    private static int minute = 0;
    private static int count1 = 0;
    private static int count2 = 0;

    public static void main(String[] args) throws FileNotFoundException, InterruptedException, ScriptException {

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

        final String topic = "coin.quotation.trade.detail";
        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        QuotationSinkFactory.registSink(new IQuotationSink() {
            @Override
            public void invoke(List<QuotationMsg> list) {
                for (QuotationMsg quotationMsg : list) {

//                    System.out.println("【"+quotationMsg);

//                    if(quotationMsg instanceof TradeDetailMsg){
//                        TradeDetailMsg data = (TradeDetailMsg) quotationMsg;
//                        JSONObject item;
//                        for (int i = 0; i < data.getData().size(); i++) {
//                            item = data.getData().getJSONObject(i);
//
//                            int m = (int )(item.getLongValue("t") / 60000);
//                            if(m > minute) {
//                                minute = m;
//                                count1 = 0;
//                                count2 = 0;
//                            }
//                            if(i == 0){
//                                count1++;
//                            }
//                            count2++;
//
//                            System.out.println(minute * 60 + " " + count1 + " " + count2);
//
////                            System.out.println(FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(item.getLongValue("t"))
////                            + " " + item.getBigDecimal("p").toPlainString() + " " + item.getBigDecimal("a").toPlainString());
//                        }
//                    }

                    if(SocketEventType.TRADE_DETAIL.name().equals(quotationMsg.getType())){
                        producer.send(new ProducerRecord(topic, quotationMsg.toString()));
                    }
                }
            }
        });

        ScriptEngineManager sem = new ScriptEngineManager();
        ScriptEngine engine = sem.getEngineByName("nashorn");
        engine.eval(new FileReader("D:\\idea-workspace\\QuotationForCoin\\datacenter\\src\\main\\resources\\engineimpl/huobiTradeDetail.js"));
        Invocable invokeEngine = (Invocable) engine;
        ICaimaoSocketListener l = invokeEngine.getInterface(ICaimaoSocketListener.class);
        WebSocketHandler huobiWebSocketHandler = new WebSocketHandler(l);
        huobiWebSocketHandler.start();

        Thread.sleep(Integer.MAX_VALUE);
    }
}
