package com.genx.quotation.collector.handler;

import com.genx.quotation.collector.msg.QuotationMsg;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2018/7/20 14:54
 */
public class QuotationEventWork implements Runnable {
    private IDataHandler dataHandler;
    private Object data;


    public <T> QuotationEventWork(IDataHandler dataHandler, T data) {
        this.dataHandler = dataHandler;
        this.data = data;
    }

    @Override
    public void run() {
        if (dataHandler != null && data != null && QuotationSinkFactory.getInstance() != null) {
            try {
                List<QuotationMsg> list = dataHandler.onHandle(data);
                if (list != null && list.size() > 0) {
                    QuotationSinkFactory.getInstance().invoke(list);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
