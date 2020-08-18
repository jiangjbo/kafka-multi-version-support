package com.jbo;

import com.jbo.kafka.multiversion.support.util.KafkaTest;
import org.apache.kafka.common.PartitionInfo;

import java.util.*;

public class Test {

    public static void main(String[] args){

        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.106.24:9092");
        props.put("client.id", UUID.randomUUID().toString());
        props.put("group.id", "test2");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "10000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaTest<String, String> consumer = new KafkaTest<>(props);
        Map<String, List<PartitionInfo>> list = consumer.listTopics();
        consumer.subscribe(Collections.singletonList("test"));
        while (true) {
            List records = consumer.poll(100L);
            records.forEach(System.out::println);
        }
    }
}
