package com.kmwllc.lucille.filetraverser.data.producer;

import com.kmwllc.lucille.core.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class OpenCSVDocumentProducerTest {

  @Test
  public void testRowsWithBlanks() throws Exception {
    OpenCSVDocumentProducer producer = new OpenCSVDocumentProducer(false);
    List<Document> docs = producer.produceDocuments(Paths.get(
      ClassLoader.getSystemResource("test.csv").toURI()), new Document("test1"));
    assertEquals(5,docs.size());

    Document doc1 = Document.fromJsonString("{\"id\":\"1\",\"csvLineNumber\":1,\"field1\":\"foo\",\"field2\":\"bar\",\"field3\":\"baz\"}");
    assertEquals(doc1, docs.get(0));

    Document doc5 = Document.fromJsonString("{\"id\":\"5\",\"csvLineNumber\":6,\"field1\":\"abc\",\"field2\":\"def\",\"field3\":\"ghi\"}");
    assertEquals(doc5, docs.get(4));
  }

  @Test
  public void testHeaderOnly() throws Exception {
    OpenCSVDocumentProducer producer = new OpenCSVDocumentProducer(false);
    List<Document> docs = producer.produceDocuments(Paths.get(
      ClassLoader.getSystemResource("test2.csv").toURI()), new Document("test1"));
    assertTrue(docs.isEmpty());
  }

  @Test
  public void testAllBlank() throws Exception {
    OpenCSVDocumentProducer producer = new OpenCSVDocumentProducer(false);
    List<Document> docs = producer.produceDocuments(Paths.get(
      ClassLoader.getSystemResource("test3.csv").toURI()), new Document("test1"));
    assertTrue(docs.isEmpty());
  }

}
