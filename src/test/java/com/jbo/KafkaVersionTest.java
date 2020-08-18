package com.jbo;

import com.jbo.kafka.multiversion.support.factory.KafkaVersionFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * @Author jiangbo
 * @Date 2020/8/18 22:24
 * @Version 1.0
 * @Description
 */
public class KafkaVersionTest {

    @Test
    public void testVersion() {
        String address = "172.16.106.223:9092";
        String version = KafkaVersionFactory.me().getKafkaVersion(address);
        Assert.assertEquals(version, "2.4.1");
    }

}
