package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.IOException;
import java.net.URI;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExponentialBackoffRetryHandlerTest {

  private int requestCounter;
  private CloseableHttpClient httpClient;

  @Before
  public void setUp() {
    requestCounter = 0;
  }

  private void createFailingHttpClient() {
    this.httpClient = HttpClientBuilder
        .create()
        .addInterceptorFirst((HttpRequestInterceptor) (request, context) -> requestCounter++)
        .setRetryHandler(new ExponentialBackoffRetryHandler(2, 500, 10000))
        .addInterceptorLast((HttpResponseInterceptor) (response, context) -> { throw new IOException(); })
        .build();
  }

  @Test
  public void test2retries() {
    createFailingHttpClient();
    assertThrows(IOException.class, () -> httpClient.execute(new HttpGet("https://httpstat.us")));
    assertEquals(3, requestCounter);
  }

  public void createHttpClientNoRetries() {
    this.httpClient = HttpClientBuilder
        .create()
        .addInterceptorFirst((HttpRequestInterceptor) (request, context) -> requestCounter++)
        .setRetryHandler(new ExponentialBackoffRetryHandler(0, 1000, 10000))
        .build();
  }

  @Test
  public void testNoRetries() throws IOException {
    createHttpClientNoRetries();
    HttpGet request = new HttpGet(URI.create("https://httpstat.us/500"));

    CloseableHttpResponse response = assertDoesNotThrow(() -> httpClient.execute(request));
    assertEquals(500, response.getStatusLine().getStatusCode());

    assertEquals(1, requestCounter);
    response.close();
  }

  @After
  public void tearDown() throws IOException {
    if (httpClient != null) {
      httpClient.close();
    }
  }

}
