package com.genx.quotation.collector.handler;

import com.genx.quotation.collector.msg.QuotationMsg;

import java.util.Calendar;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2019/1/8 20:39
 */
public class QuotationSinkFactory {

    private static IQuotationSink QUOTATION_SINK = null;

    private QuotationSinkFactory() {
    }

    public static synchronized void registSink(IQuotationSink quotationSink) {

        if (QUOTATION_SINK != null) {
            throw new RuntimeException("IQuotationSink is already registed");
        }
        QUOTATION_SINK = quotationSink;
    }

    public static IQuotationSink getInstance() {
        return QUOTATION_SINK;
    }
}
