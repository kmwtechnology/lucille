package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

// todo see if can mock server
public class EmbedTextTest {

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

  @Test
  public void test() throws StageException {
    Stage s = StageFactory.of(EmbedText.class).get("EmbedTextTest/config.conf");

    Document d = Document.create("id");
    d.setField("first", "hello world");
    d.setField("second", "hello there");

    s.start();
    s.processDocument(d);

    assertEquals(384, d.getDoubleList("first_embedded").size());
    assertEquals(384, d.getDoubleList("second_embedded").size());
  }
}