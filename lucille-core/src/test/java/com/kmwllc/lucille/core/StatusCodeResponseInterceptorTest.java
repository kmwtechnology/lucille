package com.kmwllc.lucille.core;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class StatusCodeResponseInterceptorTest {

  @Test
  public void testListContains() {
    List<String> statusCodeList = Arrays.asList("206", "429");
    List<String> statusCodeWildcardList = Arrays.asList("30", "5");
    // initial statusCodeRetryList in FetchUri would be: ["206", "30x", "429", "5XX"]
    StatusCodeResponseInterceptor statusCodeResponseInterceptor = new StatusCodeResponseInterceptor(statusCodeList, statusCodeWildcardList);

    int codeForXX = 500;
    assertTrue(statusCodeResponseInterceptor.listContainsValue(codeForXX));

    int codeForx = 307;
    assertTrue(statusCodeResponseInterceptor.listContainsValue(codeForx));

    int codeForNum = 429;
    assertTrue(statusCodeResponseInterceptor.listContainsValue(codeForNum));
  }

}
