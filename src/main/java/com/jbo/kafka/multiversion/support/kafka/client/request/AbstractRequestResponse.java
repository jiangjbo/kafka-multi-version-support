package com.jbo.kafka.multiversion.support.kafka.client.request;


import com.jbo.kafka.multiversion.support.kafka.client.protocol.types.Struct;

import java.nio.ByteBuffer;

public abstract class AbstractRequestResponse {
    protected final Struct struct;


    public AbstractRequestResponse(Struct struct) {
        this.struct = struct;
    }

    public Struct toStruct() {
        return struct;
    }

    /**
     * Get the serialized size of this object
     */
    public int sizeOf() {
        return struct.sizeOf();
    }

    /**
     * Write this object to a buffer
     */
    public void writeTo(ByteBuffer buffer) {
        struct.writeTo(buffer);
    }

    @Override
    public String toString() {
        return struct.toString();
    }

    @Override
    public int hashCode() {
        return struct.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AbstractRequestResponse other = (AbstractRequestResponse) obj;
        return struct.equals(other.struct);
    }
}
