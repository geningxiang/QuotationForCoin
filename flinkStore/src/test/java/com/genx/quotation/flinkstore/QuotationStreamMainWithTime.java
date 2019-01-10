package com.genx.quotation.flinkstore;

import com.genx.quotation.flinkstore.transform.QuotationTradeTansformer;
import com.genx.quotation.flinkstore.vo.QuotationKlineItem;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2019/1/8 22:30
 */
public class QuotationStreamMainWithTime {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        env.enableCheckpointing(5000); // 非常关键，一定要设置启动检查点！！

        //以 eventTime 为基准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //从kafka 获取数据流
        DataStreamSource<String> stream = env.readTextFile("D:/work/2.txt");


        // 将String 转成 元组
        SingleOutputStreamOperator<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>> stream1 = stream.flatMap(new QuotationTradeTansformer());

        KeyedStream<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>, Tuple> keyedStream = stream1.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>>() {

            /**
             * 保存当前收到数据的最大值
             */
            private long currentMaxTimestamp = 0L;

            /**
             * 允许延迟3秒的数据进入
             */
            private long delayAllowTimestamp = 3000L;

            @Override
            public long extractTimestamp(Tuple5<Integer, String, Long, BigDecimal, BigDecimal> item, long previousElementTimestamp) {
                if(item.f2 > currentMaxTimestamp) {
                    currentMaxTimestamp = item.f2;
                }
                return item.f2;
            }

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                //return the watermark as current highest timestamp minus the out-of-orderness bound
                return new Watermark(currentMaxTimestamp - delayAllowTimestamp);
//                return null;
            }
        }).keyBy(0, 1);


        //扩展WindowAssigner类来实现自定义窗口分配器
        WindowedStream<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.minutes(1L));

        SingleOutputStreamOperator<QuotationKlineItem> result = windowedStream.aggregate(new AggregateFunction<Tuple5<Integer, String, Long, BigDecimal, BigDecimal>, QuotationKlineItem, QuotationKlineItem>(){
            /**
             * IN - The type of the values that are aggregated (input values)
             * ACC - The type of the accumulator (intermediate aggregate state).
             * OUT - The type of the aggregated result
             */

            @Override
            public QuotationKlineItem createAccumulator() {
                return null;
            }

            @Override
            public QuotationKlineItem add(Tuple5<Integer, String, Long, BigDecimal, BigDecimal> in, QuotationKlineItem quotationKlineItem) {
//                System.out.println("【add】" + data);
                if(quotationKlineItem == null) {
                    quotationKlineItem = new QuotationKlineItem(in.f0, in.f1, in.f2, in.f3, in.f4);
                } else {
                    quotationKlineItem.update(in.f2, in.f3, in.f4);
                }
                return quotationKlineItem;
            }

            @Override
            public QuotationKlineItem getResult(QuotationKlineItem quotationKlineItem) {
                return quotationKlineItem;
            }

            /**
             * Merges two accumulators, returning an accumulator with the merged state.
             * This function may reuse any of the given accumulators as the target for the merge and return that. The assumption is that the given accumulators will not be used any more after having been passed to this function.
             * @param quotationKlineItem
             * @param acc
             * @return
             */
            @Override
            public QuotationKlineItem merge(QuotationKlineItem quotationKlineItem, QuotationKlineItem acc) {
                //TODO  merage 会在什么时候触发?  聚合?
                return null;
            }
        });


        result.print();

        // run the prediction pipeline
        env.execute("QuotationStreamMain");
    }
}
