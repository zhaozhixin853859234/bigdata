package com.partitioner;

import com.sun.org.apache.regexp.internal.RE;
import com.sun.xml.internal.ws.encoding.policy.MtomPolicyMapConfigurator;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <h3>bigdata</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-07-21 14:35
 */
// 自定义分区,通过配置文件加载自定义分区器类
public class MyPartitioner implements Partitioner {
    public int partition(String topic, Object key, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
//        Integer integer = cluster.partitionCountForTopic(topic);
//        return key.toString().hashCode() % integer;
        return 1;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }

}
