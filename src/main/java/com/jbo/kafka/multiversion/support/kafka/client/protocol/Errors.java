package com.jbo.kafka.multiversion.support.kafka.client.protocol;

import com.jbo.kafka.multiversion.support.kafka.client.exception.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This class contains all the client-server errors--those errors that must be sent from the server to the client. These
 * are thus part of the protocol. The names can be changed but the error code cannot.
 *
 * Do not add exceptions that occur only on the client or only on the server here.
 */
public enum Errors {
    NONE(0, null);

    private static final Logger log = LoggerFactory.getLogger(Errors.class);

    private static Map<Class<?>, Errors> classToError = new HashMap<Class<?>, Errors>();
    private static Map<Short, Errors> codeToError = new HashMap<Short, Errors>();

    static {
        for (Errors error : Errors.values()) {
            codeToError.put(error.code(), error);
            if (error.exception != null)
                classToError.put(error.exception.getClass(), error);
        }
    }

    private final short code;
    private final ApiException exception;

    private Errors(int code, ApiException exception) {
        this.code = (short) code;
        this.exception = exception;
    }

    /**
     * An instance of the exception
     */
    public ApiException exception() {
        return this.exception;
    }

    /**
     * Returns the class name of the exception or null if this is {@code Errors.NONE}.
     */
    public String exceptionName() {
        return exception == null ? null : exception.getClass().getName();
    }

    /**
     * The error code for the exception
     */
    public short code() {
        return this.code;
    }

    /**
     * Throw the exception corresponding to this error if there is one
     */
    public void maybeThrow() {
        if (exception != null) {
            throw this.exception;
        }
    }

    /**
     * Get a friendly description of the error (if one is available).
     * @return the error message
     */
    public String message() {
        if (exception != null)
            return exception.getMessage();
        return toString();
    }

}
