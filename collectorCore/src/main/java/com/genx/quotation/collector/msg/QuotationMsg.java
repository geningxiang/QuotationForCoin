package com.genx.quotation.collector.msg;

import com.alibaba.fastjson.JSON;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2019/1/7 23:50
 */
public abstract class QuotationMsg {
    public abstract String getType();

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
