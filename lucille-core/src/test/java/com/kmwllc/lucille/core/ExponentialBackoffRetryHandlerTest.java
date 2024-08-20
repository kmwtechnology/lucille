package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicStatusLine;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ExponentialBackoffRetryHandlerTest {

  @Test
  public void test2retries() throws Exception {
    List<String> requestArray = new ArrayList<>();

    CloseableHttpClient realHttpClient = HttpClientBuilder
        .create()
        .addInterceptorFirst((HttpRequestInterceptor) (request, context) -> requestArray.add(request.toString()))
        .setRetryHandler(new ExponentialBackoffRetryHandler(2, 500, 10000))
        .addInterceptorLast((HttpResponseInterceptor) (response, context) -> { throw new IOException(); })
        .build();

    CloseableHttpClient httpClientSpy = spy(realHttpClient);

    // give a bad request
    HttpGet request = new HttpGet("https://doesnotexist.kmwllc.com");
    
    assertThrows(IOException.class, () -> httpClientSpy.execute(request));

    // test that the client calls execute once, but requested 3 times total given 2 test retries
    verify(httpClientSpy, times(1)).execute(any(HttpGet.class));
    assertEquals(3, requestArray.size());
  }

  @Test
  public void testNoRetries() throws IOException {
    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);

    when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(mockResponse);
    when(mockResponse.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 500, "Internal Server Error"));

    HttpGet request = new HttpGet(URI.create("https://httpstat.us/500"));
    CloseableHttpResponse response = assertDoesNotThrow(() -> mockHttpClient.execute(request));

    assertEquals(500, response.getStatusLine().getStatusCode());

    response.close();

    verify(mockHttpClient, times(1)).execute(any(HttpGet.class));
  }
}
