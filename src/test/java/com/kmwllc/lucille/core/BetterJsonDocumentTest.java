package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.joda.time.Instant;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.junit.Assert.assertEquals;

public class BetterJsonDocumentTest extends DocumentTest {

  @Override
  public Document createDocument(ObjectNode node) throws DocumentException {
    return new BetterJsonDocument(node);
  }

  @Override
  public Document createDocument(String id) {
    return new BetterJsonDocument(id);
  }

  @Override
  public Document createDocument(String id, String runId) {
    return new BetterJsonDocument(id, runId);
  }

  @Override
  public Document createDocumentFromJson(String json)
      throws DocumentException, JsonProcessingException {
    return BetterJsonDocument.fromJsonString(json);
  }

  @Override
  public Document createDocumentFromJson(String json, UnaryOperator<String> idUpdater)
      throws DocumentException, JsonProcessingException {
    return BetterJsonDocument.fromJsonString(json, idUpdater);
  }

  @Test
  public void testGetDate() throws JsonProcessingException {
    // note this method is not in the interface
    BetterJsonDocument d = new BetterJsonDocument("123");
    Date date = new Date();
    d.addDate("date", date);
    assertEquals(date, d.getDate("date"));
  }

  @Test
  public void test() {
    String str = "hello";

    System.out.println(str.getClass());

    Object obj = str;

    System.out.println(obj.getClass());

    System.out.println(((String) obj).getClass());
  }

  @Test
  public <T> void testTypes() throws JsonProcessingException {

    // todo see if this is more efficient then using string conversion
    Map<String, T> map = new HashMap<>();

    map.put("key", (T) new Date());
    map.put("key2", (T) new Instant());

    Map<String, Object> map2 = new HashMap<>();

    map2.put("key", new Date());
    map2.put("key2", new Instant());
    System.out.println();

    // string to object lists
    // keep id seperately
    // hashmaps are not thread safe
    // look at jackson serializer
    // can have a million of documents with random field types and values
    // test serializing into and from json different implementations of document
  }
}
