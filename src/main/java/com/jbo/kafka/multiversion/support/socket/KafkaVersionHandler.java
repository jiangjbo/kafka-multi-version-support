package com.jbo.kafka.multiversion.support.socket;

import com.jbo.kafka.multiversion.support.kafka.client.protocol.ApiKeys;
import com.jbo.kafka.multiversion.support.kafka.client.protocol.ApiVersion;
import com.jbo.kafka.multiversion.support.kafka.client.protocol.Protocol;
import com.jbo.kafka.multiversion.support.kafka.client.protocol.types.Struct;
import com.jbo.kafka.multiversion.support.kafka.client.response.ApiVersionsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;

import static com.jbo.kafka.multiversion.support.kafka.client.protocol.Protocol.REQUEST_HEADER;


/**
 * @author jiangbo
 * @create 2018-01-07 15:06
 * @desc
 **/
public class KafkaVersionHandler {

    private final static Logger logger = LoggerFactory.getLogger(KafkaVersionHandler.class);
    private final static Object lock = new Object();
    private static volatile KafkaVersionHandler kafkaVersionHandler;
    private KafkaVersionHandler(){}

    public static KafkaVersionHandler me(){
        if(kafkaVersionHandler == null){
            synchronized (lock){
                if(kafkaVersionHandler == null){
                    kafkaVersionHandler = new KafkaVersionHandler();
                }
            }

        }
        return  kafkaVersionHandler;
    }

    /**
     * 获取指定address的version信息
     */
    public List<ApiVersion> getApiVersion(String host, int port) throws IOException {
        short keyVersion = 0;
        int correlationId = 0;
        ByteBuffer requestBuffer = apiVersionRequest(keyVersion, correlationId);
        ByteBuffer responseBuffer = send(host, port, requestBuffer);
        List<ApiVersion> apiKeyVersions = apiVersionResponse(responseBuffer);
        logger.info("%s:%d api version info %s", host, port, apiKeyVersions);
        return apiKeyVersions;
    }

    /**
     * 发送请求的方法
     */
    private ByteBuffer send(String host, int port, ByteBuffer byteBuffer) throws IOException {
        try (Socket socket = new Socket(host, port)) {
            byte[] buffer = byteBuffer.array();
            //发送请求并等待响应
            sendRequest(socket, buffer);
            byte[] response = getResponse(socket);
            return ByteBuffer.wrap(response);
        }
    }

    /**
     * 创建获取api version的请求
     */
    private ByteBuffer apiVersionRequest(short keyVersion, int correlationId){
        Struct struct = new Struct(REQUEST_HEADER);
        struct.set(REQUEST_HEADER.get("api_key"), ApiKeys.API_VERSIONS.id);
        struct.set(REQUEST_HEADER.get("api_version"), keyVersion);
        struct.set(REQUEST_HEADER.get("client_id"), UUID.randomUUID().toString());
        struct.set(REQUEST_HEADER.get("correlation_id"), correlationId);
        ByteBuffer requestBuffer = ByteBuffer.allocate(struct.sizeOf());
        struct.writeTo(requestBuffer);
        return requestBuffer;
    }

    /**
     * 解析响应头，响应体
     */
    private List<ApiVersion> apiVersionResponse(ByteBuffer responseBuffer){
        Protocol.RESPONSE_HEADER.read(responseBuffer);
        ApiVersionsResponse response = ApiVersionsResponse.parse(responseBuffer);
        return new ArrayList<>(response.apiVersions());
    }

    /**
     * 发送序列化请求给socket
     */
    private void sendRequest(Socket socket, byte[] request) throws IOException {
        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        dataOutputStream.writeInt(request.length);
        dataOutputStream.write(request);
        dataOutputStream.flush();
    }

    /**
     * 从给定socket处获取response
     */
    private byte[] getResponse(Socket socket) throws IOException {
        DataInputStream dataInputStream = null;
        try {
            socket.setSoTimeout(10*1000);
            dataInputStream = new DataInputStream(socket.getInputStream());
            byte[] response = new byte[dataInputStream.readInt()];
            dataInputStream.readFully(response);
            return response;
        } finally {
            if (dataInputStream != null) {
                dataInputStream.close();
            }
        }
    }
}
