package com.threads;

/**
 * <h3>bigdata</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-09-12 11:18
 */
public class Machine extends Thread{
    private int a =1;
    public void print(){
        System.out.println("a = "+a);
    }

    public void run(){
        synchronized (this){
            try {
                System.out.println("run  a = "+a);
                Thread.sleep(5000);
            }catch (InterruptedException e){
                throw new RuntimeException();
            }
            a++;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Machine m = new Machine();
        // 线程对象调用Start方法后，没有立即获取到该对象的锁，
        // 此时调用后面的synchronized方法，可以执行，执行完后再执行run方法
        m.start();
        Thread.sleep(10);
        Thread.yield();
        m.print();
    }
}
