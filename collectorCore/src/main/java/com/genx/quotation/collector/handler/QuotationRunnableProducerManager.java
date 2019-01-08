package com.genx.quotation.collector.handler;

import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * Description:
 *
 * @author: genx
 * @date: 2017-11-28 17:10
 */
public class QuotationRunnableProducerManager {

    private final static int MAX_SIZE = 10000;
    private final static int REMOVE_SIZE = 1000;


    private static ExecutorService EXECUTOR_SERVICE = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() > 1 ? Runtime.getRuntime().availableProcessors() : 1, Runtime.getRuntime().availableProcessors(), 30, TimeUnit.SECONDS, new ArrayBlockingQueue<>(MAX_SIZE), new MyDiscardOldestPolicy());


    public static void publishEvent(IDataHandler dataHandler, Object data) {
        try {
            EXECUTOR_SERVICE.execute(new QuotationEventWork(dataHandler, data));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class MyDiscardOldestPolicy implements RejectedExecutionHandler {
        public MyDiscardOldestPolicy() {
        }

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            if (!e.isShutdown()) {
                //队列满的时候 移除 1000个
                e.getQueue().drainTo(new ArrayBlockingQueue(REMOVE_SIZE), REMOVE_SIZE);
                e.execute(r);
            }
        }
    }
}
