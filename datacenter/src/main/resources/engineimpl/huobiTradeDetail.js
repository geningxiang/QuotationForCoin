var CompressUtil = Java.type('com.genx.quotation.collector.utils.CompressUtil');
var JSON = Java.type('com.alibaba.fastjson.JSON');
var collectorName = function () {
    return "火币-交易明细订阅";
}

var getSocketUrl = function () {
    return "wss://api.huobi.br.com/ws";
};

var onOpen = function (webSocket, response) {
    webSocket.send('{"sub": "market.ethbtc.trade.detail"}');
};

var onMessage = function (webSocket, bytes) {
    var data = CompressUtil.uncompress(bytes.toByteArray());
    //print(data);
    var json = JSON.parseObject(data);
    if (json.ping) {
        webSocket.send('{"pong": "' + json.ping + '"}');
        return null;
    } else {
        return json;
    }

};

var onHandle = function (data) {
    var ch = data.ch;
    if (ch && ch.indexOf(".trade.detail") > 0) {
        var d = data.tick.data;
        var item;
        for (var i = 0; i < d.length; i++) {
            item = d[i];
            print(item);
        }
    }
};
