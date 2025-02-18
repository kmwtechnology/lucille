package com.kmwllc.lucille.core;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class StatusCodeResponseInterceptorTest {

  @Test
  public void testListContains() {
    List<String> statusCodeRetryList = Arrays.asList("206", "30x", "429", "5xx");
    StatusCodeResponseInterceptor statusCodeResponseInterceptor = new StatusCodeResponseInterceptor(statusCodeRetryList);

    int codeForXX = 500;
    assertTrue(statusCodeResponseInterceptor.listContainsValue(codeForXX));

    int codeForx = 307;
    assertTrue(statusCodeResponseInterceptor.listContainsValue(codeForx));

    int codeForNum = 429;
    assertTrue(statusCodeResponseInterceptor.listContainsValue(codeForNum));
  }

}
