package com.genx.quotation.collector.handler;

import com.genx.quotation.collector.msg.QuotationMsg;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2019/1/8 20:39
 */
public interface IQuotationSink {

    default void invoke(List<QuotationMsg> list) {

    }
}
