package com.genx.quotation.collector.socket;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2018/7/16 17:56
 */
public enum SocketEventType {
    /**
     * 心跳
     */
    PING,

    PONG,
    /**
     * 登录
     */
    LOGIN,

    /**
     * 交易详情
     */
    TRADE_DETAIL,
    /**
     * @Author:luzhengxian
     * Description  数据补充
     * @Date:
     */
    SUPPLE_DATA,


    /**
     * 委托深度 全部  例如 火币
     */
    DEPTH_ALL,
    /**
     * 委托深度 初始化
     */
    DEPTH_INIT,
    /**
     * 委托深度变动
     */
    DEPTH_UPDATE
}
