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
  private final List<String> statusCodeList;
  private final List<String> statusCodeWildcardList;

  public StatusCodeResponseInterceptor() {
    this.statusCodeList = new ArrayList<>();
    this.statusCodeWildcardList = new ArrayList<>();
  }

  public StatusCodeResponseInterceptor(List<String> statusCodeRetryList) {
    List<String> statusCodeList = new ArrayList<>();
    List<String> statusCodeWildcardList = new ArrayList<>();
    for (String statusCode : statusCodeRetryList) {
      if (statusCode.contains("x")) {
        statusCodeWildcardList.add(statusCode.replaceAll("x", ""));
      } else {
        statusCodeList.add(statusCode);
      }
    }
    this.statusCodeList = statusCodeList;
    this.statusCodeWildcardList = statusCodeWildcardList;
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

  // check if the status code retry lists contains the code itself or a wildcard that contains includes that code.
  // ex. if code=500 and statusCodeRetryList=["429","5xx"], then this would return true.
  protected boolean listContainsValue(int code) {
    String codeStr = String.valueOf(code);
    if (this.statusCodeList.contains(codeStr)) {
      return true;
    }
    return this.statusCodeWildcardList.stream().anyMatch(codeStr::startsWith);
  }
}

