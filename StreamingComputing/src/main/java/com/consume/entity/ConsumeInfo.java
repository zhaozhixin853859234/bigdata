package com.consume.entity;

/**
 * <h3>bigdata</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-08-21 16:27
 */
public class ConsumeInfo {
    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getCredit_id() {
        return credit_id;
    }

    public void setCredit_id(String credit_id) {
        this.credit_id = credit_id;
    }

    public String getCredit_type() {
        return credit_type;
    }

    public void setCredit_type(String credit_type) {
        this.credit_type = credit_type;
    }

    public String getConsume_time() {
        return consume_time;
    }

    public void setConsume_time(String consume_time) {
        this.consume_time = consume_time;
    }

    public String getConsume_city() {
        return consume_city;
    }

    public void setConsume_city(String consume_city) {
        this.consume_city = consume_city;
    }

    public String getConsume_type() {
        return consume_type;
    }

    public void setConsume_type(String consume_type) {
        this.consume_type = consume_type;
    }

    public String getConsume_money() {
        return consume_money;
    }

    public void setConsume_money(String consume_money) {
        this.consume_money = consume_money;
    }

    private String user_id;
    private String credit_id ;
    private String credit_type;
    private String consume_time;
    private String consume_city;
    private String consume_type ;
    private String consume_money;

    public ConsumeInfo(String user_id,String credit_id,String credit_type,String consume_time,String consume_city,String consume_type,String consume_money){
        this.user_id = user_id;
        this.credit_id = credit_id;
        this.credit_type = credit_type;
        this.consume_time = consume_time;
        this.consume_city = consume_city;
        this.consume_type = consume_type;
        this.consume_money = consume_money;

    }
}
