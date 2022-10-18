package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class EmbedTextTest {

  @Test
  public void test() throws StageException {
    Stage s = StageFactory.of(EmbedText.class).get("EmbedTextTest/config.conf");

    Document d = Document.create("id");
    d.setField("text", "hello world");

    s.start();
    s.processDocument(d);

    List<Double> embedding = d.getDoubleList("text_embedding");
    assertEquals(384, embedding.size());
  }

  @Test
  public void test2() throws StageException {
    Stage s = StageFactory.of(EmbedTextV2.class).get("EmbedTextTest/config_v2.conf");

    Document d = Document.create("id");
    d.setField("first", "hello world");
    d.setField("second", "hello there");

    s.start();
    s.processDocument(d);

    assertEquals(384, d.getDoubleList("first_embedded").size());
    assertEquals(384, d.getDoubleList("second_embedded").size());
  }
}