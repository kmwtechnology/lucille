package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class EmbedTextTest {

  @Test
  public void test() throws StageException, DocumentException, JsonProcessingException {
    Stage s = StageFactory.of(EmbedText.class).get("EmbedTextTest/config.conf");

    Document d = Document.createFromJson("{\"id\": \"123\", \"text\": \"hello world\"}");

    s.start();
    s.processDocument(d);

    List<Double> embedding = d.getDoubleList("text_embedding");
    assertEquals(384, embedding.size());
  }
}