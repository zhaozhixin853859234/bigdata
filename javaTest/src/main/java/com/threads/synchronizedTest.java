package com.threads;

/**
 * <h3>bigdata</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-09-12 10:43
 */
public class synchronizedTest {
    public static void main(String[] args) throws InterruptedException {
        A a = new A();
        a.getId();
        Thread.yield();
        a.getName();
//        Runnable r1=()->{
//            System.out.println("r1开始执行");
//            a.getId();
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            System.out.println("r1执行完成");
//        };
//
//        Runnable r2 = ()->{
//            System.out.println("r2开始执行");
//            a.getName();
//            System.out.println("r2执行完成");
//        };
        new Thread(()->{
            System.out.println("t2开始执行");
            a.getName();
            System.out.println("t2执行完成");
        }).start();
//
//        Thread t1 = new Thread(r1);
//        Thread t2 = new Thread(r2);
//        t1.start();
//        t2.start();


    }
}


class A{
    private int id =1;
    private String name = "zzx";

    public synchronized int getId() throws InterruptedException {
        System.out.println("调用synchronized修饰的getId方法");
        //Thread.sleep(10000);
        return this.id;
    }

    public synchronized String getName(){
        System.out.println("调用未加synchronized修饰的getName方法");
        return this.name;
    }

}