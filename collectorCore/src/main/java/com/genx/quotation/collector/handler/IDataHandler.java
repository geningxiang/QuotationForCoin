package com.genx.quotation.collector.handler;

import com.genx.quotation.collector.msg.QuotationMsg;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2019/1/7 22:58
 */
public interface IDataHandler {

    List<QuotationMsg> onHandle(Object object);
}
