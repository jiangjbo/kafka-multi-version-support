package com.jbo.kafka.multiversion.support.consumer.impl

import com.jbo.kafka.multiversion.support.consumer.IKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class KafkaConsumer<K,V> implements IKafkaConsumer<K,V> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class)

    private org.apache.kafka.clients.consumer.KafkaConsumer<K,V> consumer

    KafkaConsumer(Properties conf) {
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(conf)
    }

    @Override
    void subscribe(Collection<String> topics) {
        consumer.subscribe(topics)
    }

    @Override
    List<V> polls(long timeout) {
        ConsumerRecords<K,V> consumerRecords = consumer.poll(timeout)
        List<V> records = new ArrayList<>()
        for(ConsumerRecord<K,V> record : consumerRecords){
            records.add(record.value())
        }
        return records
    }

    @Override
    void close() {
        consumer.close()
    }
}

