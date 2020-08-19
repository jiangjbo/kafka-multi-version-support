package com.jbo.kafka.multiversion.support.conf;


import com.jbo.kafka.multiversion.support.factory.KafkaVersionFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class KafkaConfiguration {

    private static Logger logger = LoggerFactory.getLogger(KafkaConfiguration.class);

    private static final Object lock = new Object();

    private String path = System.getProperty("kafka.multi.version.support.dir") + File.separator + "kafka";
    private String jsonPath;
    private String classesPath;
    private String groovyPath;
    private String versionHandlerPath;

    private void initPath() {
        this.jsonPath = path + File.separator + "json";
        this.classesPath = path + File.separator + "libs";
        this.groovyPath = path + File.separator + "groovy";
        this.versionHandlerPath = path + File.separator + "kafka_version_handler.json";
    }

    // key -> kafka address, value -> kafka version
    private Map<String, String> addressVersion = new HashMap<>();

    private KafkaConfiguration() {
    }

    private static volatile KafkaConfiguration kafkaConfiguration;

    public static KafkaConfiguration builder() {
        if (kafkaConfiguration == null) {
            synchronized (lock) {
                if (kafkaConfiguration == null) {
                    kafkaConfiguration = new KafkaConfiguration();
                }
            }
        }
        return kafkaConfiguration;
    }

    public void build() {
        // 初始化配置路径
        kafkaConfiguration.initPath();
        // 根据ip获取对应的kafka version
        for (String address : addressVersion.keySet()) {
            String version = addressVersion.get(address);
            try {
                if (StringUtils.isBlank(version)) {
                    version = KafkaVersionFactory.me().getKafkaVersion(address);
                    addressVersion.put(address, version);
                }
            } catch (Exception e) {
                logger.error("", e);
            }
        }
    }

    public KafkaConfiguration setPath(String path) {
        this.path = path;
        return this;
    }

    public KafkaConfiguration setHost(String addresses) {
        addressVersion.put(addresses, null);
        return this;
    }

    public KafkaConfiguration setHost(String addresses, String version) {
        addressVersion.put(addresses, version);
        return this;
    }


    public String getJsonPath() {
        return jsonPath;
    }

    public String getClassesPath() {
        return classesPath;
    }

    public String getGroovyPath() {
        return groovyPath;
    }

    public String getVersionHandlerPath() {
        return versionHandlerPath;
    }

    public Map<String, String> getAddressVersion() {
        return addressVersion;
    }
}
