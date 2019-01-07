package com.genx.quotation.collector.handler;

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
        if (dataHandler != null && data != null) {
            try{
                dataHandler.onHandle(data);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
