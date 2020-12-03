package com.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * <h3>bigdata</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-07-21 16:29
 */
public class ConsumerCommitAsyncTest {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // 连接的kafka集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"master:9092");
        // 开启自动提交offset，如果不自动提交偏移量，下一次会从kafka的_consumer_offset 默认存储offset主题
        // 读取offset，会发生重复读取数据
//        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        // 自动提交延迟
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        // 反序列化类
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        // 消费者组,重置消费者组才能从头消费数据，--from-beginning效果
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"bd1");

        // 重置消费者offset
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // 2、创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // 3、订阅消费主题
        consumer.subscribe(Arrays.asList("first","ss"));

        while (true) {
            // 4、获取数据
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            // 5、解析数据，处理相关业务
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.topic() + "----" + consumerRecord.key() + "---" + consumerRecord.value());
            }
            // 异步提交offset
            consumer.commitAsync(new OffsetCommitCallback() {
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    if (e!=null)
                        e.printStackTrace();
                }
            });
        }
    }
}
