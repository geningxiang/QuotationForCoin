package com.genx.quotation.flinkstore.vo;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;

import java.math.BigDecimal;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2019/1/8 22:31
 */
public class QuotationKlineItem {

    private int exchangeCode;
    private String symbol;


    private long minTimestamp;
    private long maxTimestamp;

    private int minute;

    private BigDecimal open;
    private BigDecimal close;

    private BigDecimal low;
    private BigDecimal high;

    /**
     * 成交量
     */
    private BigDecimal amount;

    private long num;

    /**
     * 成交额
     */
    private BigDecimal vol;

    public QuotationKlineItem(int exchangeCode, String symbol, long timestamp, BigDecimal price, BigDecimal amount) {
        this.exchangeCode = exchangeCode;
        this.symbol = symbol;
        this.minute = (int)(timestamp / 60000);
        this.open = price;
        this.close = price;
        this.low = price;
        this.high = price;
        this.amount = amount;
        this.num = 1;
        this.vol = price.multiply(amount);

        minTimestamp = timestamp;
        maxTimestamp = timestamp;
    }

    @Deprecated
    public void update(BigDecimal price, BigDecimal amount){
        this.close = price;
        if(price.compareTo(this.low) < 0){
            this.low = price;
        }
        if(price.compareTo(this.high) > 0){
            this.high = price;
        }
        this.amount = this.amount.add(amount);
        this.num++;
        this.vol = this.vol.add(price.multiply(amount));
    }

    /**
     * 更新操作 应该是支持乱序的
     * @param timestamp
     * @param price
     * @param amount
     */
    public void update(long timestamp, BigDecimal price, BigDecimal amount){
        this.close = price;
        if(price.compareTo(this.low) < 0){
            this.low = price;
        }
        if(price.compareTo(this.high) > 0){
            this.high = price;
        }
        this.amount = this.amount.add(amount);
        this.num++;
        this.vol = this.vol.add(price.multiply(amount));

        //以下是支持乱序的逻辑
        if(timestamp < this.minTimestamp){
            this.minTimestamp = timestamp;
            this.open = price;
        }
        if(timestamp > this.maxTimestamp){
            this.maxTimestamp = timestamp;
            this.close = price;
        }
    }

    public int getExchangeCode() {
        return exchangeCode;
    }

    public String getSymbol() {
        return symbol;
    }

    public int getMinute() {
        return minute;
    }

    public BigDecimal getOpen() {
        return open;
    }

    public BigDecimal getClose() {
        return close;
    }

    public BigDecimal getLow() {
        return low;
    }

    public BigDecimal getHigh() {
        return high;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public long getNum() {
        return num;
    }

    public BigDecimal getVol() {
        return vol;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("(").append(exchangeCode)
                .append(",").append(symbol)
                .append(",").append(FastDateFormat.getInstance("yyyy-MM-dd HH:mm").format(this.minute * 60000L))
                .append(",second=").append(this.minute * 60)
                .append(",开=").append(open)
                .append(",高=").append(high)
                .append(",低=").append(low)
                .append(",收=").append(close)
                .append(",交易笔数=").append(num)
                .append(",量=").append(amount)
                .append(",额=").append(vol)
                .append(")");
        return sb.toString();
    }
}
