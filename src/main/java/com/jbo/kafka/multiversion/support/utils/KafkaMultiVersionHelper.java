package com.jbo.kafka.multiversion.support.utils;


import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jiangbo
 * @create 2018-01-12 17:35
 * @desc
 **/
public class KafkaMultiVersionHelper {

    public static Map<String, Integer> transferAddress(String address) {
        Map<String, Integer> ipPort = new HashMap<>();
        Arrays.stream(StringUtils.split(address, ",")).forEach(s -> {
            String[] strs = StringUtils.split(s, ":");
            if (strs.length == 2) {
                ipPort.put(strs[0], Integer.parseInt(strs[1]));
            }
        });
        return ipPort;
    }

}
