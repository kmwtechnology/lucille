package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.StageException;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;


public class FetchUriTest {

  private CloseableHttpClient mockClient;

  @Before
  public void setup() {
    mockClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = Mockito.mock(CloseableHttpResponse.class);
    HttpEntity mockEntity = Mockito.mock(HttpEntity.class);
    StatusLine mockStatusLine = Mockito.mock(StatusLine.class);

    try {
      Mockito.when(mockClient.execute(Mockito.any(HttpGet.class))).thenReturn(mockResponse);
      Mockito.when(mockResponse.getEntity()).thenReturn(mockEntity);
      Mockito.when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
      Mockito.when(mockStatusLine.getStatusCode()).thenReturn(200);
      Mockito.when(mockEntity.getContent()).thenReturn(IOUtils.toInputStream("exampleresponse", "UTF-8"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFetchUriWithAllOptionalParameters() throws StageException {
    FetchUri s = (FetchUri) StageFactory.of(FetchUri.class).get("FetchUriTest/allOptionalParameters.conf");
    s.setClient(mockClient);
    Document d = Document.create("id");
    d.setField("name", "Jane Doe"); // extra field to test that not all fields are being read
    d.setField("url", "https://example.com"); // uri field that is meant to be read

    s.processDocument(d);

    byte[] expectedResult;
    try {
      expectedResult = IOUtils.toByteArray(IOUtils.toInputStream("exampleresponse", "UTF-8"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    assertArrayEquals(expectedResult, d.getBytes("url_data"));
    assertEquals(Integer.valueOf(200), d.getInt("url_code"));
    assertEquals(Integer.valueOf(15), d.getInt("url_length"));
  }

  @Test
  public void testFetchUriWithMaxSize() throws StageException {
    FetchUri s = (FetchUri) StageFactory.of(FetchUri.class).get("FetchUriTest/maxSizeLimit.conf");
    s.setClient(mockClient);
    Document d = Document.create("id");
    d.setField("name", "Jane Doe");
    d.setField("url", "https://example.com");

    s.processDocument(d);

    byte[] expectedResult;
    try {
      expectedResult = IOUtils.toByteArray(IOUtils.toInputStream("examplerespons", "UTF-8"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    assertArrayEquals(expectedResult, d.getBytes("url_data"));
    assertEquals(Integer.valueOf(200), d.getInt("url_status_code"));
    assertEquals(Integer.valueOf(14), d.getInt("url_size"));
  }

  @Test
  public void testFetchUriWithError() throws StageException, IOException {
    FetchUri s = (FetchUri) StageFactory.of(FetchUri.class).get("FetchUriTest/allOptionalParameters.conf");
    s.setClient(mockClient);
    Document d = Document.create("id");
    d.setField("name", "Jane Doe");
    d.setField("url", "https://example.com");

    ClientProtocolException fakeError = new ClientProtocolException("fake error");
    Mockito.when(mockClient.execute(Mockito.any(HttpGet.class))).thenThrow(fakeError);

    s.processDocument(d);

    assertEquals(null, d.getBytes("url_data"));
    assertEquals(null, d.getInt("url_code"));
    assertEquals(null, d.getInt("url_length"));
    assertEquals("org.apache.http.client.ClientProtocolException fake error", d.getString("url_error_msg"));
  }

  @Test
  public void testFetchUriWithMalformedLink() throws StageException, IOException {
    FetchUri s = (FetchUri) StageFactory.of(FetchUri.class).get("FetchUriTest/allOptionalParameters.conf");
    //s.setClient(mockClient); Not setting client, seeing that we want to ignore the setup
    Document d = Document.create("id");
    d.setField("name", "Jane Doe");
    d.setField("url", "abcdef");

    s.processDocument(d);

    // We expect that only the error field is populated with the pertinent error
    assertFalse(d.has("url_data"));
    assertFalse(d.has("url_code"));
    assertFalse(d.has("url_length"));
    assertEquals("java.net.MalformedURLException no protocol: abcdef", d.getString("url_error_msg"));
  }

  @Test
  public void testFetchUriWithNoLink() throws StageException, IOException {
    FetchUri s = (FetchUri) StageFactory.of(FetchUri.class).get("FetchUriTest/allOptionalParameters.conf");
    s.setClient(mockClient);
    Document d = Document.create("id");
    d.setField("name", "Jane Doe");
    d.setField("url", "");

    ClientProtocolException fakeError = new ClientProtocolException("fake error");
    Mockito.when(mockClient.execute(Mockito.any(HttpGet.class))).thenThrow(fakeError);

    s.processDocument(d);

    // We expect that none of the fields are populated if there is no URI/URL
    assertFalse(d.has("url_data"));
    assertFalse(d.has("url_code"));
    assertFalse(d.has("url_length"));
    assertFalse(d.has("url_error_msg"));
  }

  @Test
  public void testFetchUriWithHeaders() throws Exception {
    FetchUri s = (FetchUri) StageFactory.of(FetchUri.class).get("FetchUriTest/headers.conf");
    s.setClient(mockClient);
    Document d = Document.create("id");
    d.setField("url", "https://example.com"); // uri field that is meant to be read
    s.processDocument(d);

    ArgumentCaptor<HttpGet> httpGetCaptor = ArgumentCaptor.forClass(HttpGet.class);
    verify(mockClient, times(1)).execute(httpGetCaptor.capture());

    HttpGet httpGet = httpGetCaptor.getValue();
    assertEquals(2, httpGet.getAllHeaders().length);
    assertEquals("header1-value", httpGet.getFirstHeader("header1-name").getValue());
    assertEquals("header2-value", httpGet.getFirstHeader("header2-name").getValue());
  }
}
