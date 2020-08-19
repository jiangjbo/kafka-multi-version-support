package com.jbo.kafka.multiversion.support.consumer.impl

import com.jbo.kafka.multiversion.support.consumer.IKafkaConsumer
import com.jbo.kafka.multiversion.support.util.KafkaConsumerV8

class KafkaConsumerImpl<K,V> implements IKafkaConsumer<K,V> {

    private KafkaConsumerV8<K, V> kafkaConsumer;

    KafkaConsumerImpl(Properties conf) {
        consumer = new KafkaConsumerV8<>(conf)
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
