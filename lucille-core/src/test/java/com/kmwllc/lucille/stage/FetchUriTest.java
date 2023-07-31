package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertArrayEquals;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.StageException;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


public class FetchUriTest {

  private CloseableHttpClient mockClient;

  @Before
  public void setup() {
    mockClient = Mockito.mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = Mockito.mock(CloseableHttpResponse.class);
    HttpEntity mockEntity =  Mockito.mock(HttpEntity.class);

    try {
      Mockito.when(mockClient.execute(Mockito.any(HttpGet.class))).thenReturn(mockResponse);
      Mockito.when(mockResponse.getEntity()).thenReturn(mockEntity);
      Mockito.when(mockEntity.getContent()).thenReturn(IOUtils.toInputStream("exampleresponse", "UTF-8"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  @Test
  public void testFetchUri() throws StageException {
    FetchUri s = (FetchUri) StageFactory.of(FetchUri.class).get("FetchUriTest/config.conf");
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
  }

}
