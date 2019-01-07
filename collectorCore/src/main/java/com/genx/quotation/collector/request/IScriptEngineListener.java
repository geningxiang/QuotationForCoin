package com.genx.quotation.collector.request;

import okhttp3.Response;
import okhttp3.WebSocket;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2018/12/20 9:43
 */
public interface IScriptEngineListener {


    /**
     * socket的地址
     * @return
     */
    String getSocketUrl();

    /**
     * @param webSocket
     * @param response
     */
    void onOpen(WebSocket webSocket, Response response);

    String decompress();

    void onEvent(WebSocketHandler webSocketHandler, String text);
}
