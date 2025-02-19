package com.kmwllc.lucille.core.fileHandler;

public class FileHandlerException extends Exception {

  public FileHandlerException(String message) {
    super(message);
  }

  public FileHandlerException(String message, Throwable cause) {
    super(message, cause);
  }

  public FileHandlerException(Throwable cause) {
    super(cause);
  }

  protected FileHandlerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public FileHandlerException() {
    super();
  }
}
