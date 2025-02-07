package com.kmwllc.lucille.core;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusCodeResponseInterceptor implements HttpResponseInterceptor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private int numRetry = 0;
  private final List<String> statusCodeRetryList;

  public StatusCodeResponseInterceptor() {
    this.statusCodeRetryList = new ArrayList<>();
  }

  public StatusCodeResponseInterceptor(List<String> statusCodeRetryList) {
    this.statusCodeRetryList = statusCodeRetryList;
  }

  @Override
  public void process(HttpResponse httpResponse, HttpContext httpContext) throws IOException {
    HttpClientContext clientContext = HttpClientContext.adapt(httpContext);
    int statusCode = httpResponse.getStatusLine().getStatusCode();
    numRetry++;
    if (listContainsValue(statusCode)) {
      log.error("Got status {} after {} retries", statusCode, numRetry - 1);
      throw new IOException("Retry request to '" + clientContext.getTargetHost() + clientContext.getRequest().getRequestLine().getUri() + "'");
    }
  }

  // check if this statusCodeRetryList contains the code itself or a wildcard that contains includes that code.
  // ex. if code=500 and statusCodeRetryList=["429","5xx"], then this would return true.
  protected boolean listContainsValue(int code) {
    String codeStr = String.valueOf(code);
    if (this.statusCodeRetryList.contains(codeStr)) {
      return true;
    }
    for (String statusCode : this.statusCodeRetryList) {
      statusCode = statusCode.toLowerCase();
      if (statusCode.contains("x")) {
        if (codeStr.startsWith(statusCode.replaceAll("x", ""))) {
          return true;
        }
      }
    }
    return false;
  }
}

