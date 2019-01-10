package com.genx.quotation.flinkstore.rich;

import com.genx.quotation.flinkstore.vo.QuotationKlineItem;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2019/1/10 15:25
 */
public class MinuteKLineRichFunction extends RichFlatMapFunction<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>, QuotationKlineItem> {
    private transient ValueState<QuotationKlineItem> modelState;

    @Override
    public void flatMap(Tuple5<Integer, String, Long, BigDecimal, BigDecimal> data, Collector<QuotationKlineItem> out) throws IOException {
        if (modelState.value() == null) {
            modelState.update(new QuotationKlineItem(data.f0, data.f1, data.f2, data.f3, data.f4));
        } else {
            int minute = (int) (data.f2 / 60000);
            if (minute < modelState.value().getMinute()) {

            } else if (minute == modelState.value().getMinute()) {
                //分钟内的更新
                modelState.value().update(data.f3, data.f4);
            } else {
                out.collect(modelState.value());

                modelState.update(new QuotationKlineItem(data.f0, data.f1, data.f2, data.f3, data.f4));
            }
        }
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<QuotationKlineItem> descriptor =
                new ValueStateDescriptor(
                        "quotationKline",
                        TypeInformation.of(QuotationKlineItem.class));
        modelState = getRuntimeContext().getState(descriptor);
    }
}
