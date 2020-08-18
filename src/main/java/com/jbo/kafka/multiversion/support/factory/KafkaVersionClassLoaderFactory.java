package com.jbo.kafka.multiversion.support.factory;

import com.jbo.kafka.multiversion.support.conf.KafkaConfiguration;
import com.jbo.kafka.multiversion.support.conf.LoaderVersionHandlerClasses;
import com.jbo.kafka.multiversion.support.consumer.IKafkaConsumer;
import com.jbo.kafka.multiversion.support.entry.KafkaVersionClassLoader;
import com.jbo.kafka.multiversion.support.entry.KafkaVersionHandlerInfo;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyCodeSource;
import org.apache.commons.collections4.CollectionUtils;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.jbo.kafka.multiversion.support.utils.LambdaExceptionUtil.rethrowConsumer;

/**
 * @author jiangbo
 * @create 2018-01-08 19:42
 * @desc
 **/
public class KafkaVersionClassLoaderFactory {

    private static final Logger logger = LoggerFactory.getLogger(KafkaVersionClassLoaderFactory.class);

    private Map<String, KafkaVersionClassLoader> kafkaVersionClassLoaders = new ConcurrentHashMap<>();

    private static KafkaVersionClassLoaderFactory kvcf;

    private List<KafkaVersionHandlerInfo> kafkaVersionClassJars;

    public static KafkaVersionClassLoaderFactory me() {
        if (kvcf == null) {
            synchronized (KafkaVersionClassLoaderFactory.class) {
                if (kvcf == null) {
                    KafkaConfiguration conf = KafkaConfiguration.builder();
                    //加载jar包和groovy配置
                    kvcf = new KafkaVersionClassLoaderFactory();
                    LoaderVersionHandlerClasses loaderVersion = new LoaderVersionHandlerClasses(conf.getClassesPath(),
                            conf.getGroovyPath(), conf.getVersionHandlerPath());
                    kvcf.kafkaVersionClassJars = loaderVersion.getKafkaVersionClassJarInfo();
                }
            }
        }
        return kvcf;
    }

    //自定义的version, 需要获取classloader
    public KafkaVersionClassLoader getKafkaVersionClassLoader(String version) {
        KafkaVersionClassLoader kafkaVersionClassLoader = kafkaVersionClassLoaders.get(version);
        if (kafkaVersionClassLoader == null) {
            synchronized (KafkaVersionClassLoaderFactory.class) {
                kafkaVersionClassLoader = createKafkaVersionClassLoader(version);
                kafkaVersionClassLoaders.put(version, kafkaVersionClassLoader);
            }
        }
        return kafkaVersionClassLoader;
    }

    private KafkaVersionClassLoader createKafkaVersionClassLoader(String version) {
        try {
            ClassLoader kafkaClassLoader = new KafkaClassLoader(version);
            KafkaVersionClassLoader kafkaKafkaVersionClassLoader;
            List<File> groovyFiles = getGroovyFiles(version);
            //未使用groovy
            if (CollectionUtils.isEmpty(groovyFiles)) {
                kafkaKafkaVersionClassLoader = new KafkaVersionClassLoader(version, kafkaClassLoader);
            } else {
                CompilerConfiguration compilerConfiguration = new CompilerConfiguration();
                compilerConfiguration.setSourceEncoding(Charset.defaultCharset().name());
                GroovyClassLoader groovyClassLoader = new GroovyClassLoader(kafkaClassLoader, compilerConfiguration);
                groovyFiles.forEach(rethrowConsumer(groovyFile ->
                        groovyClassLoader.parseClass(new GroovyCodeSource(groovyFile, Charset.defaultCharset().name()), true)));
                kafkaKafkaVersionClassLoader = new KafkaVersionClassLoader(version, kafkaClassLoader, groovyClassLoader);
            }
            return kafkaKafkaVersionClassLoader;
        } catch (IOException e) {
            logger.error("", e);
            throw new RuntimeException(e);
        }
    }

    private Class<?> getLoaderClass(KafkaVersionClassLoader kafkaClassLoader, String clazzName) throws Exception {
        Class<?> clazz;
        if (kafkaClassLoader.isUseGroovy()) {
            clazz = kafkaClassLoader.getGroovyClassLoader().loadClass(clazzName);
        } else {
            clazz = kafkaClassLoader.getKafkaClassLoader().loadClass(clazzName);
        }
        return clazz;
    }

    /**
     * 获取kafka consumer
     */
    public IKafkaConsumer kafkaConsumer(String version, Properties conf) {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();

        KafkaVersionClassLoader kafkaClassLoader = getKafkaVersionClassLoader(version);
        Thread.currentThread().setContextClassLoader(kafkaClassLoader.getKafkaClassLoader());
        IKafkaConsumer kafkaConsumer = null;
        try {
            String consumerClass = getConsumerClass(version);
            Class<?> clazz = getLoaderClass(kafkaClassLoader, consumerClass);
            kafkaConsumer = (IKafkaConsumer) clazz.getConstructor(Properties.class).newInstance(conf);
        } catch (Exception e) {
            String message = String.format("kafka版本[ %s ]对应的处理类不存在: ", version);
            logger.error(message, e);
            throw new RuntimeException(message);
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
        return kafkaConsumer;
    }

    private List<File> getGroovyFiles(String version) {
        Optional<List<File>> optional = kafkaVersionClassJars.stream()
                .filter(kafkaVersionClassJar -> kafkaVersionClassJar.getVersion().equals(version) && kafkaVersionClassJar.getGroovy() != null)
                .map(KafkaVersionHandlerInfo::getGroovy)
                .findFirst();
        return optional.orElseGet(Collections::emptyList);
    }

    private final String consumerClass = "com.jbo.kafka.multiversion.support.consumer.impl.KafkaConsumerAdapt_%s";
    private String getConsumerClass(String version) throws Exception {
        Optional<String> optional = kafkaVersionClassJars.stream()
                .filter(kafkaVersionClassJar -> kafkaVersionClassJar.getVersion().equals(version))
                .map(kafkaVersionHandlerInfo -> String.format(consumerClass, kafkaVersionHandlerInfo.getId()))
                .findFirst();
        return optional.orElseThrow(() -> new Exception(String.format("%s, 未定义处理类", version)));
    }

    class KafkaClassLoader extends URLClassLoader {

        private final Logger logger = LoggerFactory.getLogger(KafkaClassLoader.class);

        private KafkaClassLoader(String version) {
            super(new URL[]{}, null);
            loadResource(version);
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            return super.loadClass(name);
        }

        @Override
        protected Class<?> findClass(String name) throws ClassNotFoundException {
            try {
                return super.findClass(name);
            } catch (ClassNotFoundException e) {
                return KafkaClassLoader.class.getClassLoader().loadClass(name);
            } catch (Exception e) {
                logger.warn(String.format("%s 未找到", e.getMessage()));
                return KafkaClassLoader.class.getClassLoader().loadClass(name);
            }
        }

        private void loadResource(String version) {
            kafkaVersionClassJars.stream()
                    .filter(kafkaVersionClassJar -> kafkaVersionClassJar.getVersion().equals(version))
                    .filter(kafkaVersionClassJar -> kafkaVersionClassJar.getJars() != null)
                    .map(KafkaVersionHandlerInfo::getJars)
                    .findFirst()
                    .ifPresent(files -> files.forEach(this::loadJar));
        }

        private void loadJar(File file) {
            if (!file.exists() || !file.isFile()) {
                return;
            }
            addURLs(file);
        }

        private void addURLs(File file) {
            try {
                addURL(file.toURI().toURL());
            } catch (IOException e) {
                logger.warn("加载jar包失败：", e.getMessage());
            }
        }

    }

}
