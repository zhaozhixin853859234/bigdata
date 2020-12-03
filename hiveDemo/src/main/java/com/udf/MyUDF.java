package com.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * <h3>bigdata</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-09-10 16:42
 */
public class MyUDF extends UDF {
    // 注意函数名字不能改变，可以重载，hive采用反射机制找到方法，getField（）
    public int evaluate(int data){
        return data+ 5;
    }


}
