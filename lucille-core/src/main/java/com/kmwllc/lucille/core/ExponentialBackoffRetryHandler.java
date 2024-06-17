package com.kmwllc.lucille.core;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExponentialBackoffRetryHandler implements HttpRequestRetryHandler {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final int maxNumRetries;
  private final int initialExpiry;
  private final int maxExpiry;

  public ExponentialBackoffRetryHandler(int maxNumRetries, int initialExpiry, int maxExpiry) {
    this.maxNumRetries = maxNumRetries;
    this.initialExpiry = initialExpiry;
    this.maxExpiry = maxExpiry;
  }

  @Override
  public boolean retryRequest(IOException exception, int numRetries, HttpContext httpContext) {
    if (numRetries > this.maxNumRetries) {
      return false;
    } else {
      long delay = (long) Math.min(this.initialExpiry * Math.pow(2, numRetries - 1), this.maxExpiry);
      HttpClientContext clientContext = HttpClientContext.adapt(httpContext);
      log.info("Retrying request path '{}'........waiting {}ms", clientContext.getRequest().getRequestLine().getUri(), delay);
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return true;
    }
  }
}

