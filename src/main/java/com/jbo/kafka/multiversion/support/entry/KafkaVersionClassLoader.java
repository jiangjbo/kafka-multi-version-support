package com.jbo.kafka.multiversion.support.entry;

import groovy.lang.GroovyClassLoader;

/**
 * @author jiangbo
 * @create 2018-01-08 19:39
 * @desc
 **/
public class KafkaVersionClassLoader {

    private String version;
    private ClassLoader kafkaClassLoader;
    private GroovyClassLoader groovyClassLoader;
    private boolean isUseGroovy;

    public KafkaVersionClassLoader(String version, ClassLoader kafkaClassLoader) {
        this(version, kafkaClassLoader, null);
    }

    public KafkaVersionClassLoader(String version, ClassLoader kafkaClassLoader, GroovyClassLoader groovyClassLoader) {
        this.version = version;
        this.kafkaClassLoader = kafkaClassLoader;
        if(groovyClassLoader != null){
            this.groovyClassLoader = groovyClassLoader;
            this.isUseGroovy = true;
        }
    }

    public String getVersion() {
        return version;
    }

    public ClassLoader getKafkaClassLoader() {
        return kafkaClassLoader;
    }

    public GroovyClassLoader getGroovyClassLoader() {
        return groovyClassLoader;
    }

    public boolean isUseGroovy() {
        return isUseGroovy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KafkaVersionClassLoader)) return false;

        KafkaVersionClassLoader that = (KafkaVersionClassLoader) o;

        return version != null ? version.equals(that.version) : that.version == null;
    }

    @Override
    public int hashCode() {
        return version != null ? version.hashCode() : 0;
    }

}
