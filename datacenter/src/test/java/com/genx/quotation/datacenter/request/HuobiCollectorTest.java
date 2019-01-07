package com.genx.quotation.datacenter.request;

import com.genx.quotation.collector.request.ICaimaoSocketListener;
import com.genx.quotation.collector.request.WebSocketHandler;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *  以 engine js 的方式 订阅 火币的行情数据
 * @author: genx
 * @date: 2019/1/7 22:18
 */
public class HuobiCollectorTest {

    public static void main(String[] args) throws FileNotFoundException, InterruptedException, ScriptException {
        ScriptEngineManager sem = new ScriptEngineManager();
        ScriptEngine engine = sem.getEngineByName("nashorn");


        engine.eval(new FileReader("D:\\idea-workspace\\QuotationForCoin\\datacenter\\src\\main\\resources\\engineimpl/huobiTradeDetail.js"));

        Invocable invokeEngine = (Invocable) engine;
        ICaimaoSocketListener l = invokeEngine.getInterface(ICaimaoSocketListener.class);


        WebSocketHandler huobiWebSocketHandler = new WebSocketHandler("scriptEngine测试", l);
        huobiWebSocketHandler.start();

        Thread.sleep(Integer.MAX_VALUE);
    }
}
