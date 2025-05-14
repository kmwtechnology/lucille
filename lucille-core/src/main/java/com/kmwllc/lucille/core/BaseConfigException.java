package com.kmwllc.lucille.core;

public class BaseConfigException extends Exception {

    public BaseConfigException(String message) {
        super(message);
    }

    public BaseConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public BaseConfigException() {
        super();
    }   

    public BaseConfigException(Throwable cause) {
        super(cause);
    }

    protected BaseConfigException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
    
}
