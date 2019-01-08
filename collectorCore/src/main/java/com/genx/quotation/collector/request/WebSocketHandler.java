package com.genx.quotation.collector.request;

import com.genx.quotation.collector.handler.QuotationRunnableProducerManager;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2018/12/18 13:25
 */
public class WebSocketHandler extends WebSocketListener {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final String name;
    private final ICaimaoSocketListener caimaoSocketListener;


    /**
     * 实时处理数据
     * @param caimaoSocketListener
     */
    public WebSocketHandler(ICaimaoSocketListener caimaoSocketListener) {
        this.name = caimaoSocketListener.collectorName();
        this.caimaoSocketListener = caimaoSocketListener;
    }


    private WebSocket currentWebSocket = null;

    public synchronized void start() {
        if (currentWebSocket == null) {
            String socketUrl = caimaoSocketListener != null ? caimaoSocketListener.getSocketUrl() : null;
            if (StringUtils.isNotEmpty(socketUrl)) {
                Request request = new Request.Builder().url(socketUrl).build();
                logger.info("【{}】{}", this.name, socketUrl);
                currentWebSocket = RequestClientFactory.getOkHttpClient().newWebSocket(request, this);

            } else {
                throw new IllegalArgumentException("请先定义socket地址");
            }
        }
    }

    public void close() {
        this.close(1000, "主动关闭");
    }

    public synchronized void close(int code, String reason) {
        if (currentWebSocket != null) {
            currentWebSocket.close(code, reason);
        }
    }

    /**
     * 为了安全 不允许重写
     *
     * @param webSocket
     * @param response
     */
    @Override
    public final void onOpen(WebSocket webSocket, Response response) {
        logger.info("【{}】onOpen", this.name);
        this.currentWebSocket = webSocket;
        if (this.caimaoSocketListener != null) {
            this.caimaoSocketListener.onOpen(webSocket, response);
        }
    }


    /**
     * 远程断开连接
     *
     * @param webSocket
     * @param code
     * @param reason
     */
    @Override
    public final void onClosing(WebSocket webSocket, int code, String reason) {
        logger.warn("【{}】onClosing:{}|{}", this.name, code, reason);
        this.currentWebSocket = null;

        //TODO
    }

    /**
     * 主动断开或因错误断开
     *
     * @param webSocket
     * @param t
     * @param response
     */
    @Override
    public final void onFailure(WebSocket webSocket, Throwable t, Response response) {
        logger.warn("【{}】onFailure:{}", this.name, t.getMessage());
        this.currentWebSocket = null;

        //TODO
    }


    /**
     * 收到数据
     *
     * @param webSocket
     * @param bytes
     */
    @Override
    public void onMessage(WebSocket webSocket, ByteString bytes) {
        if (this.caimaoSocketListener != null) {
            Object data = this.caimaoSocketListener.onMessage(webSocket, bytes);

            if(data == null) {

            } else if (data instanceof String){
                onMessage(webSocket, (String)data);
            } else {
                QuotationRunnableProducerManager.publishEvent(this.caimaoSocketListener, data);
            }
        }
    }

    /**
     * 收到数据
     *
     * @param webSocket
     * @param text
     */
    @Override
    public void onMessage(WebSocket webSocket, String text) {
        if (this.caimaoSocketListener != null) {
            Object data = this.caimaoSocketListener.onMessageWidthText(webSocket, text);
            if(data != null) {
                QuotationRunnableProducerManager.publishEvent(this.caimaoSocketListener, data);
            }
        }
    }

    public void send(String data) {
        if (this.currentWebSocket != null) {
            this.currentWebSocket.send(data);
        }
    }
}
