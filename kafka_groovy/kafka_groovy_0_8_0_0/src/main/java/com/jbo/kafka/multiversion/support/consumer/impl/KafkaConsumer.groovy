package com.jbo.kafka.multiversion.support.consumer.impl

import com.jbo.kafka.multiversion.support.consumer.IKafkaConsumer
import com.jbo.kafka.multiversion.support.util.KafkaConsumerImpl

class KafkaConsumer<K,V> implements IKafkaConsumer<K,V> {

    private KafkaConsumerImpl<K, V> kafkaConsumer;

    KafkaConsumer(Properties conf) {
        consumer = new KafkaConsumerImpl<>(conf)
    }

    @Override
    void subscribe(Collection topics) {
        kafkaConsumer.subscribe(new ArrayList<String>(topics))
    }

    @Override
    void close() {
        kafkaConsumer.close()
    }

    @Override
    List<V> polls(long timeout) {
        return kafkaConsumer.polls(timeout)
    }
}
