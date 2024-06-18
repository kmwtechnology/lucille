package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.After;
import org.junit.Test;

public class ExponentialBackoffRetryHandlerTest {

  private CloseableHttpClient httpClient;

  @Test
  public void test2retries() {
    List<String> requestArray = new ArrayList<>();
    this.httpClient = HttpClientBuilder
        .create()
        .addInterceptorFirst((HttpRequestInterceptor) (request, context) -> requestArray.add(request.toString()))
        .setRetryHandler(new ExponentialBackoffRetryHandler(2, 500, 10000))
        .addInterceptorLast((HttpResponseInterceptor) (response, context) -> { throw new IOException(); })
        .build();

    assertThrows(IOException.class, () -> httpClient.execute(new HttpGet("https://httpstat.us")));
    assertEquals(3, requestArray.size());
  }

  @Test
  public void testNoRetries() throws IOException {
    List<String> requestArray = new ArrayList<>();
    this.httpClient = HttpClientBuilder
        .create()
        .addInterceptorFirst((HttpRequestInterceptor) (request, context) -> requestArray.add(request.toString()))
        .setRetryHandler(new ExponentialBackoffRetryHandler(0, 1000, 10000))
        .build();

    HttpGet request = new HttpGet(URI.create("https://httpstat.us/500"));

    CloseableHttpResponse response = assertDoesNotThrow(() -> httpClient.execute(request));
    assertEquals(500, response.getStatusLine().getStatusCode());

    assertEquals(1, requestArray.size());
    response.close();
  }

  @After
  public void tearDown() throws IOException {
    if (httpClient != null) {
      httpClient.close();
    }
  }

}
