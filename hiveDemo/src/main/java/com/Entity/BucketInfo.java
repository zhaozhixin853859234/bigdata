package com.Entity;

import java.io.Serializable;

/**
 * <h3>BigDataTest</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-06-03 09:10
 */
// 一般Entity类需要实现序列化接口java.io.Serializable，用于网络序列化传输
// 一个Entity类对应数据库中一个表
public class BucketInfo implements Serializable {
    private static final long serialVersionUID = -6125297654796395674L;

    private String id;
    private String time;

    public BucketInfo(){}

    public BucketInfo(String id,String time){
        this.id = id;
        this.time = time;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }


}
