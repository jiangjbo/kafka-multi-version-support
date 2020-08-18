package com.jbo;

import com.jbo.kafka.multiversion.support.conf.KafkaConfiguration;
import com.jbo.kafka.multiversion.support.consumer.impl.KafkaConsumerAdapt;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerTest {

    @Test
    public void main(){
        String address = "172.16.106.223:9092";

        URL url = ClassLoader.getSystemClassLoader().getResource("kafka");
        Assert.assertNotNull(url);

        KafkaConfiguration.builder()
                .setPath(url.getPath())
                .setHost(address)
                .build();

        Properties props = new Properties();
        props.put("bootstrap.servers", address);
        props.put("group.id", "test2");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "10000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumerAdapt<String, String> consumer = new KafkaConsumerAdapt<>(props);
        consumer.subscribe(Collections.singletonList("monitor"));
        while (true) {
            List<String> records = consumer.polls(100);
            records.forEach(System.out::println);
            if(records.size() > 0) {
                return;
            }
        }
    }
}
