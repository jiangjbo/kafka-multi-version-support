package com.jbo.kafka.multiversion.support.consumer.impl;

import com.jbo.kafka.multiversion.support.consumer.IKafkaConsumer;
import com.jbo.kafka.multiversion.support.factory.KafkaVersionClassLoaderFactory;
import com.jbo.kafka.multiversion.support.factory.KafkaVersionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author jiangbo
 * @create 2018-01-07 17:35
 * @desc
 **/
public class KafkaConsumerAdapt<K,V> implements IKafkaConsumer<K,V> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerAdapt.class);

    private Properties conf;
    private IKafkaConsumer<K,V> consumer;

    public KafkaConsumerAdapt(Map<String, Object> map){
        this.conf = new Properties();
        conf.putAll(map);
        init();

    }
    public KafkaConsumerAdapt(Properties conf){
        this.conf = conf;
        init();
    }

    private void init(){
        String address = conf.getProperty("bootstrap.servers");
        String version = KafkaVersionFactory.me().getKafkaVersion(address);
        consumer = KafkaVersionClassLoaderFactory.me().kafkaConsumer(version, conf);
        logger.info("address {}, version {}, consumer init success");
    }

    @Override
    public void subscribe(Collection<String> topics){
        consumer.subscribe(topics);
    }

    @Override
    public List<V> polls(long timeout) {
        return consumer.polls(timeout);
    }

    @Override
    public void close() {
        consumer.close();
    }

}
