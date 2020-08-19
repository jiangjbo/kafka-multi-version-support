package com.jbo.kafka.multiversion.support.factory;

import com.alibaba.fastjson.JSONObject;
import com.jbo.kafka.multiversion.support.conf.KafkaConfiguration;
import com.jbo.kafka.multiversion.support.entry.KafkaVersion;
import com.jbo.kafka.multiversion.support.socket.KafkaVersionHandler;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

import static com.jbo.kafka.multiversion.support.utils.LambdaExceptionUtil.rethrowFunction;

/**
 * @Author jiangbo
 * @Date 2020/8/16 13:34
 * @Version 1.0
 * @Description
 */
public class KafkaVersionFactory {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfiguration.class);

    private static final Object lock = new Object();

    //kafka 的版本号和对应的版本接口
    private List<KafkaVersion> kafkaVersions;

    private static KafkaVersionFactory kafkaVersionFactory;

    public static KafkaVersionFactory me() {
        if (kafkaVersionFactory == null) {
            synchronized (lock) {
                if (kafkaVersionFactory == null) {
                    kafkaVersionFactory = new KafkaVersionFactory();
                    KafkaConfiguration conf = KafkaConfiguration.builder();
                    // 加载kafka版本号和对应的apiVersion
                    kafkaVersionFactory.kafkaVersions = kafkaVersionFactory.getKafkaVersions(conf.getJsonPath());
                }
            }
        }

        return kafkaVersionFactory;
    }

    public String getKafkaVersion(String address) {
        Map<String, String> addressVersion = KafkaConfiguration.builder().getAddressVersion();
        if (StringUtils.isBlank(addressVersion.get(address))) {
            synchronized (lock) {
                if (StringUtils.isBlank(addressVersion.get(address))) {
                    try {
                        String version = getKafkaVersionByAddress(address);
                        addressVersion.put(address, version);
                        KafkaVersionClassLoaderFactory.me().getKafkaVersionClassLoader(version);
                        return version;
                    } catch (Exception e) {
                        logger.error("", e);
                    }
                }
            }
        }
        return addressVersion.get(address);
    }

    private String getKafkaVersionByAddress(String address) throws Exception {
        String[] addresses = address.split(",");
        Map<String, String> versions = new HashMap<>();
        Arrays.stream(addresses).forEach(addr -> {
            String[] ads = addr.split(":");
            //通过ip和port获取kafka的apiVersion，然后和配置文件中的apiVersion比较，找到对应的kafka版本
            try {
                Optional.ofNullable(KafkaVersionHandler.me().getApiVersion(ads[0], Integer.parseInt(ads[1])))
                        .ifPresent(apiKeyVersions -> kafkaVersions.stream()
                                .filter(kafkaVersion -> CollectionUtils.isEqualCollection(apiKeyVersions, kafkaVersion.getApiKeys()))
                                .map(KafkaVersion::getVersion)
                                .max(String::compareTo)
                                .ifPresent(version -> versions.put(addr, version)));
            } catch (IOException e) {
                logger.warn(String.format("获取kafka[%s:%d]版本信息失败", ads[0], Integer.parseInt(ads[1])));
            }
        });
        if (versions.size() != addresses.length) {
            logger.warn(String.format("%s, 获取kafka版本信息失败",
                    Arrays.stream(addresses).filter(addr -> !versions.containsKey(addr)).collect(Collectors.toList())));
        }
        logger.info(String.format("kafka[%s]版本信息: %s", address, versions));
        return versions.values().stream().findFirst().orElseThrow(() -> new IOException(String.format("%s, 获取kafka版本信息失败", address)));
    }

    private List<KafkaVersion> getKafkaVersions(String jsonPath)  {
        try {
            File path = new File(jsonPath);
            if (path.isDirectory()) {
                File[] files = path.listFiles();
                if (files != null && files.length > 0) {
                    return Arrays.stream(files)
                            .map(rethrowFunction(file -> FileUtils.readFileToString(file, Charset.defaultCharset().name())))
                            .map(str -> JSONObject.parseObject(str, KafkaVersion.class))
                            .collect(Collectors.toList());
                }
            }
        } catch (IOException e) {
            String message = String.format("%s retrieval failed", jsonPath);
            logger.error(message, e);
            throw new RuntimeException(message);
        }
        return Collections.emptyList();
    }

}
