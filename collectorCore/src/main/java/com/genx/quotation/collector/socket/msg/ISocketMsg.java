package com.genx.quotation.collector.socket.msg;

import com.alibaba.fastjson.JSON;
import com.genx.quotation.collector.socket.SocketEventType;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2019/1/7 23:50
 */
public interface ISocketMsg {
    String getType();

    default String toJSONString() {
        return JSON.toJSONString(this);
    }
}
