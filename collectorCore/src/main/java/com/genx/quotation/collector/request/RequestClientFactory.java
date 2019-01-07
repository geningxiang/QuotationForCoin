package com.genx.quotation.collector.request;

import okhttp3.OkHttpClient;

import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * 用来管理 对交易的的连接
 *
 * @author: genx
 * @date: 2018/8/16 13:29
 */
public class RequestClientFactory {

    private static OkHttpClient okHttpClient = null;
    private static OkHttpClient okHttpClientOverWall = null;

    public static OkHttpClient getOkHttpClient() {
        if (okHttpClient == null) {
            synchronized (RequestClientFactory.class) {
                if (okHttpClient == null) {
                    okHttpClient = new OkHttpClient.Builder()
                            //设置读取超时时间
                            .readTimeout(30, TimeUnit.SECONDS)
                            //设置写的超时时间
                            .writeTimeout(30, TimeUnit.SECONDS)
                            //设置连接超时时间
                            .connectTimeout(30, TimeUnit.SECONDS)
                            //.proxy(QuotationConstants.FANQIANG_ONE)
                            .build();
                }
            }
        }
        return okHttpClient;
    }


//    public static OkHttpClient getOkHttpClientOverWall() {
//        if (okHttpClientOverWall == null) {
//            synchronized (RequestClientFactory.class) {
//                if (okHttpClientOverWall == null) {
//                    okHttpClientOverWall = new OkHttpClient.Builder()
//                            //设置读取超时时间
//                            .readTimeout(30, TimeUnit.SECONDS)
//                            //设置写的超时时间
//                            .writeTimeout(30, TimeUnit.SECONDS)
//                            //设置连接超时时间
//                            .connectTimeout(30, TimeUnit.SECONDS)
//                            .proxy(QuotationConstants.FANQIANG_ONE)
//                            .build();
//                }
//            }
//        }
//        return okHttpClientOverWall;
//    }

}
