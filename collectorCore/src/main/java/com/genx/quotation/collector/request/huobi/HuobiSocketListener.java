package com.genx.quotation.collector.request.huobi;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.genx.quotation.collector.msg.QuotationMsg;
import com.genx.quotation.collector.msg.TradeDetailMsg;
import com.genx.quotation.collector.request.ICaimaoSocketListener;
import com.genx.quotation.collector.utils.CompressUtil;
import okhttp3.Response;
import okhttp3.WebSocket;
import okio.ByteString;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2019/1/9 20:42
 */
public class HuobiSocketListener implements ICaimaoSocketListener {

    /**
     * 火币的行情返回 有时会有id重复的数据返回  需要剔除这部分数据
     */
    private Map<String, BigInteger> idMap = new ConcurrentHashMap<>(512);

    @Override
    public String collectorName() {
        return "火币-交易明细订阅";
    }

    @Override
    public String getSocketUrl() {
        return "wss://api.huobi.br.com/ws";
    }

    @Override
    public void onOpen(WebSocket webSocket, Response response) {
        webSocket.send("{\"sub\": \"market.btcusdt.trade.detail\"}");
        webSocket.send("{\"sub\": \"market.ethusdt.trade.detail\"}");
//        webSocket.send("{\"sub\": \"market.xrpusdt.trade.detail\"}");
    }

    @Override
    public Object onMessage(WebSocket webSocket, ByteString bytes) {
        String data = CompressUtil.uncompress(bytes.toByteArray());
        //print(data);
        JSONObject json = JSON.parseObject(data);
        if (json.containsKey("ping")) {
            //print('【ping】' + json.ping)
            webSocket.send("{\"pong\": \"" + json.getLongValue("ping") + "\"}");
            return null;
        } else if (json.containsKey("ch")) {
            return json;
        }
        return null;
    }


    @Override
    public List<QuotationMsg> onHandle(Object data) {
        if (data instanceof JSONObject) {
            String ch = ((JSONObject) data).getString("ch");
            if (ch.endsWith(".trade.detail")) {
                List<QuotationMsg> list = new ArrayList<>(1);
                String symbol = ch.substring(7, 13);
                JSONArray d = ((JSONObject) data).getJSONObject("tick").getJSONArray("data");
                TradeDetailMsg msg = new TradeDetailMsg(10004, symbol, d.size());
                JSONObject item;
                BigInteger lastId = this.idMap.get(symbol);
//                System.out.println(data);
                for (int i = 0; i < d.size(); i++) {
                    item = d.getJSONObject(i);

                    BigInteger id = item.getBigInteger("id");
//                    System.out.println(((int)(item.getLongValue("ts") / 60000) * 60) + " , " + id);
                    if(lastId != null && lastId.compareTo(id) > 0){
//                        System.out.println("【忽略】" + id);
                        //忽略
                        continue;
                    }
                    msg.addTradeDetail(item.getLongValue("ts"), item.getBigDecimal("amount").setScale(4, BigDecimal.ROUND_HALF_UP).stripTrailingZeros(), item.getBigDecimal("price").stripTrailingZeros(), "buy" == item.getString("direction") ? 1 : -1);

                    if(i == d.size() - 1){
                        this.idMap.put(symbol, id);
                    }
                }
                list.add(msg);
                return list;
            }
        }
        return null;
    }
}
