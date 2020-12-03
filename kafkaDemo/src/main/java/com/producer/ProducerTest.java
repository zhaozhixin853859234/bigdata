package com.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.cache.SynchronizedCache;

import java.util.*;

/**
 * <h3>bigdata</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-07-20 16:44
 */
public class ProducerTest {
    public static void main(String[] args) {
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
        properties.put("linger.ms",1);
        // RecordAccumulator缓冲区大小 32M
        properties.put("buffer.memory",33554432);

        // key\value 序列化类
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        // 2、创建生产者对象
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // 3、发送数据
        for (int i = 0; i <10 ; i++) {
            producer.send(new ProducerRecord<String, String>("first","zzxTest--"+i));
        }

        // 4、关闭资源
        producer.close();
    }

}
