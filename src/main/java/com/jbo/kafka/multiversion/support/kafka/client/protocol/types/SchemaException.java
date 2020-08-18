package com.jbo.kafka.multiversion.support.kafka.client.protocol.types;


import com.jbo.kafka.multiversion.support.kafka.client.exception.KafkaException;

/**
 *  Thrown if the protocol schema validation fails while parsing request or response.
 */
public class SchemaException extends KafkaException {

    private static final long serialVersionUID = 1L;

    public SchemaException(String message) {
        super(message);
    }

}
