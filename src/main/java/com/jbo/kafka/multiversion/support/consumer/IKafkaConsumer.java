package com.jbo.kafka.multiversion.support.consumer;

import java.util.Collection;
import java.util.List;

/**
 * @author jiangbo
 * @create 2018-01-12 17:35
 * @desc
 **/
public interface IKafkaConsumer<K, V> {

    void subscribe(Collection<String> topics);

    void close();

    List<V> polls(long timeout);

}
