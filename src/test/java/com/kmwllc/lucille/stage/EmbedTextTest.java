package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class EmbedTextTest {

 private static HttpClient mockClient;

  @Before
  public void setup() {
    mockClient = Mockito.mock(HttpClient.class);
    HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
    when(mockResponse.body()).thenReturn(makeResponseJson());

    try {
      when(mockClient.send(any(), any()))
        .thenReturn(mockResponse);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static String makeResponseJson() {
    Double[][] d = new Double[2][384];
    for (int i = 0; i < 384; i++) {
      d[0][i] = 0.0;
    }
    d[1] = d[0];

    return String.format("{\"model\":\"model name\",\"num_total\":1,\"embeddings\":" +
      "%s}", Arrays.deepToString(d).replaceAll(" ", ""));
  }


  static class MockEmbedText extends EmbedText {

    public MockEmbedText(Config config) {
      super(config);
    }

    @Override
    public HttpClient buildClient() {
      return mockClient;
    }
  }


  @Test
  public void testMockClient() throws StageException {
    Stage s = StageFactory.of(MockEmbedText.class).get("EmbedTextTest/config.conf");

    Document d = Document.create("id");
    d.setField("first", "hello world");
    d.setField("second", "hello there");

    s.start();
    s.processDocument(d);

    assertEquals(384, d.getDoubleList("first_embedded").size());
    assertEquals(384, d.getDoubleList("second_embedded").size());
  }

  @Test
  public void testMapOrder() {
    Map<Integer, String> map = new LinkedHashMap<>();

    map.put(1, "one");
    map.put(2, "two");
    map.put(3, "three");

    List<Integer> keys = new ArrayList<>();
    List<String> values = new ArrayList<>();
    for (Map.Entry<Integer, String> entry : map.entrySet()) {
      keys.add(entry.getKey());
      values.add(entry.getValue());
    }
    assertEquals(keys, List.of(1, 2, 3));
    assertEquals(values, List.of("one", "two", "three"));
  }


  @Test(expected = StageException.class)
  public void testInvalidConnection() throws StageException {
    Stage s = StageFactory.of(EmbedText.class).get("EmbedTextTest/invalid_connection.conf");

    Document d = Document.create("id");
    d.setField("first", "hello world");
    d.setField("second", "hello there");

    s.start();
    s.processDocument(d);
  }
}