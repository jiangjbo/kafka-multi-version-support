package com.jbo.kafka.multiversion.support.kafka.client.protocol;

public class ApiVersion {

    private short apiKey;
    private short minVersion;
    private short maxVersion;

    public ApiVersion() {
    }

    public ApiVersion(short apiKey, short minVersion, short maxVersion) {
        this.apiKey = apiKey;
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
    }

    public short getApiKey() {
        return apiKey;
    }

    public void setApiKey(short apiKey) {
        this.apiKey = apiKey;
    }

    public short getMinVersion() {
        return minVersion;
    }

    public void setMinVersion(short minVersion) {
        this.minVersion = minVersion;
    }

    public short getMaxVersion() {
        return maxVersion;
    }

    public void setMaxVersion(short maxVersion) {
        this.maxVersion = maxVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ApiVersion)) return false;

        ApiVersion that = (ApiVersion) o;

        if (apiKey != that.apiKey) return false;
        if (minVersion != that.minVersion) return false;
        return maxVersion == that.maxVersion;
    }

    @Override
    public int hashCode() {
        int result = (int) apiKey;
        result = 31 * result + (int) minVersion;
        result = 31 * result + (int) maxVersion;
        return result;
    }

    @Override
    public String toString() {
        return "ApiVersion{" +
                "apiKey=" + apiKey +
                ", minVersion=" + minVersion +
                ", maxVersion=" + maxVersion +
                '}';
    }
}
