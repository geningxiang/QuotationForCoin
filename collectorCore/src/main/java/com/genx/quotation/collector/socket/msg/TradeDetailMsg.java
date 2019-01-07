package com.genx.quotation.collector.socket.msg;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.genx.quotation.collector.socket.SocketEventType;

import java.math.BigDecimal;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2018/9/6 17:28
 */
public class TradeDetailMsg implements ISocketMsg {

    /**
     * 交易所
     */
    private int ec;
    /**
     * 交易对 统一小写
     */
    private String symbol;

    /**
     * 数据
     * [{t: 毫秒, a: 成交浪, p: 成交价, d: 买卖}]
     */
    private JSONArray data;

    @Override
    public String getType() {
        return SocketEventType.TRADE_DETAIL.name();
    }

    public TradeDetailMsg(int exchangeCode, String symbol, Integer initialCapacity) {
        this.ec = exchangeCode;
        this.symbol = symbol;
        if (initialCapacity != null && initialCapacity > 0) {
            data = new JSONArray(initialCapacity);
        } else {
            data = new JSONArray();
        }
    }

    /**
     * 添加一条 交易记录
     *
     * @param timeMillis
     * @param amonut
     * @param price
     * @param direction  1买 -1卖  0无方向(一般不会出现这种情况)
     */
    public void addTradeDetail(long timeMillis, BigDecimal amonut, BigDecimal price, int direction) {
        JSONObject item = new JSONObject();
        //交易时间戳 毫秒
        item.put("t", timeMillis);
        //成交量
        item.put("a", amonut);
        //成交价
        item.put("p", price);
        item.put("d", direction);
        data.add(item);
    }

    public int getEc() {
        return ec;
    }

    public String getSymbol() {
        return symbol;
    }

    public JSONArray getData() {
        return data;
    }


}
