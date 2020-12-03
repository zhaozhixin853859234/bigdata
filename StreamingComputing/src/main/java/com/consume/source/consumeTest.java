package com.consume.source;

import com.consume.entity.ConsumeInfo;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * <h3>bigdata</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-08-21 15:20
 */
public class consumeTest {
    public static void main(String[] args) throws IOException {
        // 1、创建消费者配置信息
        Properties properties = new Properties();
        // 连接的kafka集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092");
        // 开启自动提交offset，如果不自动提交偏移量，下一次会从kafka的_consumer_offset 默认存储offset主题
        // 读取offset，会发生重复读取数据
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        // 自动提交延迟
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        // 反序列化类
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // 消费者组,重置消费者组才能从头消费数据，--from-beginning效果
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"g3");

        // 重置消费者offset
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // 2、创建消费者对象
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(properties);
        // KafkaConsumer<String, ConsumeInfo> consumer1 = new KafkaConsumer<String, ConsumeInfo>(properties);

        // 3、订阅消费主题
        consumer.subscribe(Collections.singleton("consumeRecord"));

        // 4、解析schema文件
        Schema schema = new Schema.Parser().parse(new File("C:\\software\\code\\IdeaProject\\bigdata\\StreamingComputing\\src\\main\\resources\\consumeInfo.avsc"));
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

        // 5、主题中拉取数据
        int count = 1;
        Date start = new Date();
        while(count<1000000){
            // 注意注意ConsumerRecords<String,byte[]>和ConsumerRecord<String,byte[]>的区别
            // ConsumerRecords<String,byte[]>返回一个记录列表
            // 其中包含了记录所属主题的信息、记录所在分区的信息、记录在分区里的偏移量，以及记录的键值对
            // ConsumerRecord<String,byte[]> 可以获取数据的value

            ConsumerRecords<String,byte[]> records = consumer.poll(100);
            //System.out.println("本批次数据条数："+records.count());// 默认500条数据一批
            for (ConsumerRecord<String,byte[]> record:records) {
               GenericRecord genericRecord  = recordInjection.invert(record.value()).get();

               String user_id =  genericRecord.get("user_id").toString();
               String credit_id = genericRecord.get("credit_id").toString();
               String credit_type = genericRecord.get("credit_type").toString();
               String consume_time = genericRecord.get("consume_time").toString();
               String consume_city  = genericRecord.get("consume_city").toString();
               String consume_type = genericRecord.get("consume_type").toString();
               String consume_money = genericRecord.get("consume_money").toString();

//               System.out.println(count +"   "+ user_id+"   "+credit_id+"   "+credit_type+"   "+consume_time+"   "
//                       +consume_city+"   "+consume_type+"   "+consume_money);
               // count++写在for循环外时，不是取出1000条数据，而是取出1000批数据，一个ConsumerRecords会包含多条数据
               count++;
            }
            // 异步提交偏移量
            consumer.commitAsync();
            //count++;
        }
        Date end = new Date();

        System.out.println("读取"+(count-1)+"条数据，共耗时"+(end.getTime()-start.getTime())+"毫秒");
    }
}
