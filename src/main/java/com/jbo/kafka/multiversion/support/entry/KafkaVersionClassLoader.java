package com.jbo.kafka.multiversion.support.entry;

/**
 * @author jiangbo
 * @create 2018-01-08 19:39
 * @desc
 **/
public class KafkaVersionClassLoader {

    private String version;
    private ClassLoader classLoader;

    public KafkaVersionClassLoader(String version, ClassLoader classLoader) {
        this.version = version;
        this.classLoader = classLoader;
    }

    public String getVersion() {
        return version;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
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
