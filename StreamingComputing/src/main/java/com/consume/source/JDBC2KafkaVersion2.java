package com.consume.source;

import com.consume.entity.ConsumeInfo;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Properties;

/**
 * <h3>bigdata</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-09-01 10:06
 */
// 采用字符串形式将数据导入kafka，区别于自定义schema形式
public class JDBC2KafkaVersion2 {

    public static void main(String[] args) throws SQLException, InterruptedException, IOException {
        // 1.创建kafka生产者配置信息
        Properties properties = new Properties();

        // 指定连接kafka集群,本机要能识别主机名映射，否则要添加
        // properties.put("bootstrap.servers","master:9092");
        // 用ProducerConfig类查看配置参数信息,建议使用这种方法
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092");
        // 指定ack应答级别
        properties.put("acks","all");
        // 重试次数
        properties.put("retries",3);
        // 生产者一次发送数据批次大小
        properties.put("batch.size",16384);
        // 等待时间
        properties.put("linger.ms",100);
        // RecordAccumulator缓冲区大小 32M
        properties.put("buffer.memory",33554432);

        // key\value 序列化类
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        // 自定义序分区器,都加入到分区1
        // properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.partitioner.MyPartitioner");
        // 2、创建生产者对象
        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);

        // 3、获取mysql数据
        Connection conn = JDBCUtil.jdbcConnection();
        Statement stmt = conn.createStatement();

        String sql = "select * from consume_record order by consume_time";

        ResultSet rs = stmt.executeQuery(sql);

        int count = 1;
        Date start = new Date();
        // 获取mysql查询结果，需要使用avro序列化，并发送到kafka
        while (rs.next()){
            String user_id = rs.getString("user_id");
            String credit_id = rs.getString("credit_id");
            String credit_type = rs.getString("credit_type");
            String consume_time = rs.getString("consume_time");
            String consume_city = rs.getString("consume_city");
            String consume_type = rs.getString("consume_type");
            String consume_money =  rs.getString("consume_money");
//            String consumeInfo = new ConsumeInfo(user_id,credit_id,credit_type,
//                    consume_time,consume_city,consume_type,consume_money).toString();
            String consumeInfo = user_id+","+ credit_id+","+credit_type+","+consume_time+","
                    +consume_city+","+consume_type+","+consume_money;

            ProducerRecord<String, String> record = new ProducerRecord<String, String>("consumeRecordV2", user_id, consumeInfo);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // 表示写入成功
                    if (e==null){
                        System.out.println(recordMetadata.partition()+"---"+recordMetadata.offset());
                    }else {
                        System.out.println("写入失败!");
                        e.printStackTrace();
                    }
                }
            });
            System.out.println("发送第"+count+"条数据成功");
            count++;

            //Thread.sleep(100);
        }
        Date end = new Date();
        System.out.println("写入"+(count-1)+"条数据，共耗时"+(end.getTime()-start.getTime())+"毫秒");

        // 4、关闭资源
        producer.close();
    }
}
