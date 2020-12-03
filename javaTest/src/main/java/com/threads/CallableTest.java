package com.threads;

import java.util.concurrent.*;

/**
 * <h3>bigdata</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-08-05 16:03
 */
public class CallableTest implements Callable<String> {
    public static void main(String[] args) throws Exception {
        FutureTask<String> task = new FutureTask<String>(new CallableTest());
        FutureTask<String> task1 = new FutureTask<String>(new CallableTest());
        FutureTask<String> task2 = new FutureTask<String>(new CallableTest());
        // 使用单独线程执行任务
//        Thread t1 = new Thread(task);
//        t1.start();
        // 使用线程池执行任务
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<String> submit = executor.submit(new CallableTest());
        Future<String> submit1 = executor.submit(new CallableTest());

        System.out.println("等待计算结果");
        // 异步操作，线程执行完才会返回结果
        System.out.println("获取计算结果："+submit.get());
        //System.out.println("获取计算结果："+task.get());
        System.out.println("计算完成");
        executor.shutdown();

    }

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    public String call() throws Exception {
        int count=0;
        for (int i = 0; i <10 ; i++) {
            System.out.println(Thread.currentThread().getName()+"execute call:"+i);
            count = count+i;
            Thread.sleep(500);
        }
        return "over:"+count;
    }
}

