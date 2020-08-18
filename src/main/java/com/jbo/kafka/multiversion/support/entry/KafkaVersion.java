package com.jbo.kafka.multiversion.support.entry;

import com.jbo.kafka.multiversion.support.kafka.client.protocol.ApiVersion;

import java.util.List;

/**
 * @author jiangbo
 * @create 2018-01-08 19:39
 * @desc
 **/
public class KafkaVersion {

    private String version;

    private List<ApiVersion> apiKeys;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public List<ApiVersion> getApiKeys() {
        return apiKeys;
    }

    public void setApiKeys(List<ApiVersion> apiKeys) {
        this.apiKeys = apiKeys;
    }

    @Override
    public String toString() {
        return "KafkaVersion{" +
                "version='" + version + '\'' +
                ", apiKeys=" + apiKeys +
                '}';
    }

}
