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

//    public static Object getDecoder(String type,byte[] bytes){
//        if(bytes == null){
//            return null;
//        }
//        Object value = null;
//        switch(type){
//            case "StringDeserializer" :
//                value = new String(bytes);break;
//            case "ByteBufferDeserializer" :
//                value = ByteBuffer.wrap(bytes);break;
//            case "IntegerDeserializer" :
//                value = bytes.length == 0 ? null : Integer.valueOf(new String(bytes));break;
//            case "LongDeserializer" :
//                value = bytes.length == 0 ? null : Long.valueOf(new String(bytes));break;
//            case "DoubleDeserializer" :
//                value = bytes.length == 0 ? null : Double.valueOf(new String(bytes));break;
//            case "BytesDeserializer" :
//                value = Bytes.wrap(bytes);break;
//            default:
//                value = bytes;break;
//        }
//        return value;
//    }
}
