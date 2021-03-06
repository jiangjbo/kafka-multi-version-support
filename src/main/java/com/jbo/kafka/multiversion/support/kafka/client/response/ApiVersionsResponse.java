package com.jbo.kafka.multiversion.support.kafka.client.response;

import com.jbo.kafka.multiversion.support.kafka.client.protocol.*;
import com.jbo.kafka.multiversion.support.kafka.client.protocol.types.Schema;
import com.jbo.kafka.multiversion.support.kafka.client.protocol.types.Struct;
import com.jbo.kafka.multiversion.support.kafka.client.request.AbstractRequestResponse;

import java.nio.ByteBuffer;
import java.util.*;

public class ApiVersionsResponse extends AbstractRequestResponse {

    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.API_VERSIONS.id);
    private static final ApiVersionsResponse API_VERSIONS_RESPONSE = createApiVersionsResponse();

    public static final String ERROR_CODE_KEY_NAME = "error_code";
    public static final String API_VERSIONS_KEY_NAME = "api_versions";
    public static final String API_KEY_NAME = "api_key";
    public static final String MIN_VERSION_KEY_NAME = "min_version";
    public static final String MAX_VERSION_KEY_NAME = "max_version";

    /**
     * Possible error codes:
     * <p>
     * UNSUPPORTED_VERSION (33)
     */
    private final short errorCode;
    private final Map<Short, ApiVersion> apiKeyToApiVersion;

    public ApiVersionsResponse(short errorCode, List<ApiVersion> apiVersions) {
        super(new Struct(CURRENT_SCHEMA));
        struct.set(ERROR_CODE_KEY_NAME, errorCode);
        List<Struct> apiVersionList = new ArrayList<>();
        for (ApiVersion apiVersion : apiVersions) {
            Struct apiVersionStruct = struct.instance(API_VERSIONS_KEY_NAME);
            apiVersionStruct.set(API_KEY_NAME, apiVersion.getApiKey());
            apiVersionStruct.set(MIN_VERSION_KEY_NAME, apiVersion.getMinVersion());
            apiVersionStruct.set(MAX_VERSION_KEY_NAME, apiVersion.getMaxVersion());
            apiVersionList.add(apiVersionStruct);
        }
        struct.set(API_VERSIONS_KEY_NAME, apiVersionList.toArray());
        this.errorCode = errorCode;
        this.apiKeyToApiVersion = buildApiKeyToApiVersion(apiVersions);
    }

    public ApiVersionsResponse(Struct struct) {
        super(struct);
        this.errorCode = struct.getShort(ERROR_CODE_KEY_NAME);
        List<ApiVersion> tempApiVersions = new ArrayList<>();
        for (Object apiVersionsObj : struct.getArray(API_VERSIONS_KEY_NAME)) {
            Struct apiVersionStruct = (Struct) apiVersionsObj;
            short apiKey = apiVersionStruct.getShort(API_KEY_NAME);
            short minVersion = apiVersionStruct.getShort(MIN_VERSION_KEY_NAME);
            short maxVersion = apiVersionStruct.getShort(MAX_VERSION_KEY_NAME);
            tempApiVersions.add(new ApiVersion(apiKey, minVersion, maxVersion));
        }
        this.apiKeyToApiVersion = buildApiKeyToApiVersion(tempApiVersions);
    }

    public Collection<ApiVersion> apiVersions() {
        return apiKeyToApiVersion.values();
    }

    public ApiVersion apiVersion(short apiKey) {
        return apiKeyToApiVersion.get(apiKey);
    }

    public short errorCode() {
        return errorCode;
    }

    public static ApiVersionsResponse parse(ByteBuffer buffer) {
        return new ApiVersionsResponse(CURRENT_SCHEMA.read(buffer));
    }

    public static ApiVersionsResponse fromError(Errors error) {
        return new ApiVersionsResponse(error.code(), Collections.<ApiVersion>emptyList());
    }

    public static ApiVersionsResponse apiVersionsResponse() {
        return API_VERSIONS_RESPONSE;
    }

    private static ApiVersionsResponse createApiVersionsResponse() {
        List<ApiVersion> versionList = new ArrayList<>();
        for (ApiKeys apiKey : ApiKeys.values()) {
            versionList.add(new ApiVersion(apiKey.id, Protocol.MIN_VERSIONS[apiKey.id], Protocol.CURR_VERSION[apiKey.id]));
        }
        return new ApiVersionsResponse(Errors.NONE.code(), versionList);
    }

    private Map<Short, ApiVersion> buildApiKeyToApiVersion(List<ApiVersion> apiVersions) {
        Map<Short, ApiVersion> tempApiIdToApiVersion = new HashMap<>();
        for (ApiVersion apiVersion : apiVersions) {
            tempApiIdToApiVersion.put(apiVersion.getApiKey(), apiVersion);
        }
        return tempApiIdToApiVersion;
    }
}