package com.jbo.kafka.multiversion.support.entry;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jiangbo
 * @create 2018-01-08 19:39
 * @desc
 **/
public class KafkaVersionHandlerInfo {

    private String id;
    private String version;
    private List<File> jars;
    private List<File> groovy;

    public KafkaVersionHandlerInfo(String id, String version, List<File> jars, List<File> groovy) {
        this.id = id;
        this.version = version;
        this.jars = jars;
        this.groovy = groovy;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public List<File> getJars() {
        return jars;
    }

    public List<File> getGroovy() {
        return groovy;
    }

    @Override
    public String toString() {
        return "KafkaVersionHandlerInfo{" +
                "version='" + version + '\'' +
                ", jars=" + jars +
                ", groovy='" + groovy + '\'' +
                '}';
    }
}
