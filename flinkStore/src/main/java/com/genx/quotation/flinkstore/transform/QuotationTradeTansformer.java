package com.genx.quotation.flinkstore.transform;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * kafka 行情 string 转 元组
 *
 * @author: genx
 * @date: 2019/1/10 15:22
 */
public class QuotationTradeTansformer implements FlatMapFunction<String, Tuple5<Integer, String, Long, BigDecimal, BigDecimal>> {


    @Override
    public void flatMap(String s, Collector<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>> collector) throws Exception {
        try {
            JSONObject json = JSON.parseObject(s);
            int exchangeCode = json.getIntValue("ec");
            String symbol = json.getString("symbol");
            JSONArray data = json.getJSONArray("data");
            JSONObject item;
            for (int i = 0; i < data.size(); i++) {
                item = data.getJSONObject(i);
                collector.collect(new Tuple5(exchangeCode, symbol, item.getLongValue("t"), item.getBigDecimal("p"), item.getBigDecimal("a")));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

