package com.date;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * <h3>bigdata</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-08-29 10:55
 */
public class DateTest {
    public static void main(String[] args) throws InterruptedException {
        Date date1 = new Date();
        Thread.sleep(10000);
        Date date2 = new Date();
        System.out.println(date1);
        System.out.println(date2);
        System.out.println(date2.getTime()-date1.getTime());
    }
}
