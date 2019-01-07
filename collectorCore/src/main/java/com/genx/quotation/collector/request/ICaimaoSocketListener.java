package com.genx.quotation.collector.request;

import com.genx.quotation.collector.handler.IDataHandler;
import com.genx.quotation.collector.handler.QuotationRunnableProducerManager;
import okhttp3.Response;
import okhttp3.WebSocket;
import okio.ByteString;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2018/12/18 13:45
 */
public interface ICaimaoSocketListener extends IDataHandler {

    String collectorName();

    /**
     * socket的地址
     *
     * @return
     */
    String getSocketUrl();

    /**
     * 成功建立socket连接
     *
     * @param webSocket
     * @param response
     */
    void onOpen(WebSocket webSocket, Response response);

    /**
     * 收到消息  ByteString 默认转到String
     *
     * @param webSocket
     * @param bytes
     */
    default Object onMessage(WebSocket webSocket, ByteString bytes) {
        return bytes.utf8();
    }

    /**
     * 收到消息
     *
     * @param webSocket
     * @param text
     */
    default Object onMessageWidthText(WebSocket webSocket, String text) {
        return text;
    }


}
