package com.jbo.kafka.multiversion.support.conf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jbo.kafka.multiversion.support.entry.KafkaVersionHandlerInfo;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.jbo.kafka.multiversion.support.conf.KafkaVersionConstants.VERSION_PRE;

/**
 * @author jiangbo
 * @create 2018-01-12 17:35
 * @desc
 **/
public class LoaderVersionHandlerClasses {

    private static Logger logger = LoggerFactory.getLogger(LoaderVersionHandlerClasses.class);
    private List<KafkaVersionHandlerInfo> versionHandlerInfoList;

    public LoaderVersionHandlerClasses(String classesPath, String groovyPath, String kafkaVersionHandlerJson){
        try {
            File file = new File(kafkaVersionHandlerJson);
            JSONArray handlerInfoList = JSONObject.parseArray(FileUtils.readFileToString(file, Charset.defaultCharset().name()));
            versionHandlerInfoList = handlerInfoList.stream()
                    .map(o -> (JSONObject)o)
                    .map(handlerInfo -> build(classesPath, groovyPath, handlerInfo))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            logger.error("读取配置信息失败", e);
        }
    }

    public List<KafkaVersionHandlerInfo> getKafkaVersionClassJarInfo() {
        return versionHandlerInfoList;
    }

    private KafkaVersionHandlerInfo build(String classesPath, String groovyPath, JSONObject handlerInfo) {
        String id = handlerInfo.getString("id");
        String version = handlerInfo.getString("version");
        String jars = handlerInfo.getOrDefault("jars", VERSION_PRE + id).toString();
        String groovy = handlerInfo.getOrDefault("groovy", VERSION_PRE  + id).toString();
        return new KafkaVersionHandlerInfo(id, version,
                parsePath(classesPath, jars), parsePath(groovyPath, groovy));

    }

    private List<File> parsePath(String classesPath, String path){
        List<File> classesFiles = new ArrayList<>();
        File classesFile = new File(classesPath + File.separator + path);
        if(classesFile.isFile()){
            classesFiles.add(classesFile);
        } else if(classesFile.isDirectory()){
            File[] files = classesFile.listFiles();
            if(files != null && files.length > 0){
                classesFiles.addAll(Arrays.asList(files));
            }
        } else{
            logger.warn(classesPath + File.separator + path + " 目录下无需要加载的文件");
        }
        return classesFiles;
    }

}
